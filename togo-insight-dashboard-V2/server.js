require("dotenv").config();
const express = require("express");
const mongoose = require("mongoose");
const path = require("path");
const cors = require("cors");
const bcrypt = require("bcrypt");
const helmet = require("helmet");
const cookieParser = require("cookie-parser");
const rateLimit = require("express-rate-limit");
const multer = require("multer");
const jwt = require("jsonwebtoken");
const { BlobServiceClient } = require("@azure/storage-blob");
const session = require("express-session");
const { MongoStore } = require("connect-mongo");
const flash = require("connect-flash");
const XLSX = require('xlsx');

const User = require("./models/userModel");
const Contact = require("./models/contactModel");
const File = require("./models/fileModel");
const authMiddleware = require("./middleware/authMiddleware");

const app = express();

// Middleware
app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));
app.use(express.static(path.join(__dirname, "public")));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cors());
// Configure Helmet to allow inline scripts and styles
app.use(
  helmet({
    contentSecurityPolicy: {
      directives: {
        defaultSrc: ["'self'"],
        scriptSrc: ["'self'", "'unsafe-inline'", "https://unpkg.com", "https://cdn.jsdelivr.net", "https://cdnjs.cloudflare.com"],
        scriptSrcAttr: ["'unsafe-inline'", "'unsafe-hashes'"],
        styleSrc: ["'self'", "'unsafe-inline'", "https://cdnjs.cloudflare.com", "https://unpkg.com", "https://fonts.googleapis.com"],
        imgSrc: ["'self'", "data:", "https://cdn-icons-png.flaticon.com"],
        connectSrc: ["'self'", "https://cdn.jsdelivr.net", "https://cdnjs.cloudflare.com"],
        fontSrc: ["'self'", "https://cdnjs.cloudflare.com", "https://fonts.gstatic.com"],
        objectSrc: ["'none'"],
        mediaSrc: ["'self'"],
        frameSrc: ["'self'", "https://www.google.com"]
      }
    }
  })
);
app.use(cookieParser());

// Configure session middleware (MongoDB store – no MemoryStore warning, production-safe)
app.use(session({
  secret: process.env.SESSION_SECRET || "secure-session-secret-key",
  store: MongoStore.create({
    clientPromise: mongoose.connection.asPromise().then((conn) => conn.getClient()),
    dbName: mongoose.connection.name || undefined,
  }),
  resave: false,
  saveUninitialized: false,
  cookie: {
    httpOnly: true,
    secure: process.env.NODE_ENV === "production",
    maxAge: 24 * 60 * 60 * 1000 // 1 day
  }
}));

// Setup flash messages
app.use(flash());

// Make flash messages available to all templates
app.use((req, res, next) => {
  res.locals.success_msg = req.flash("success_msg");
  res.locals.error_msg = req.flash("error_msg");
  next();
});

// Rate Limiting (POST only)
const loginLimiter = rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 5,
    message: { message: "Too many login attempts. Try again later." }
});
app.use("/login", (req, res, next) => {
  if (req.method === "POST") return loginLimiter(req, res, next);
  next();
});

// MongoDB Connection – server starts only after DB is connected to avoid buffering timeouts
const mongoOptions = {
  serverSelectionTimeoutMS: 20000,
  connectTimeoutMS: 20000,
};
function connectMongo() {
  console.log("⏳ Connecting to MongoDB...");
  return mongoose.connect(process.env.MONGO_URI, mongoOptions)
    .then(() => console.log("✅ MongoDB Connected"))
    .catch(err => {
      console.log("❌ DB Connection Error:", err.message || err);
      throw err;
    });
}
connectMongo()
  .catch(() => {
    console.log("Retrying MongoDB connection in 5s...");
    return new Promise((resolve) => setTimeout(resolve, 5000)).then(connectMongo);
  })
  .catch(() => {
    console.log("Second retry in 5s...");
    return new Promise((resolve) => setTimeout(resolve, 5000)).then(connectMongo);
  })
  .catch((err) => {
    console.log("❌ MongoDB unreachable. Check network, firewall, or use Atlas 'Direct connection' in .env.");
    process.exit(1);
  });

// Azure Storage
const AZURE_STORAGE_CONNECTION_STRING = process.env.AZURE_STORAGE_CONNECTION_STRING;
const CONTAINER_NAME = "prodtogodata";
const blobServiceClient = BlobServiceClient.fromConnectionString(AZURE_STORAGE_CONNECTION_STRING);
const containerClient = blobServiceClient.getContainerClient(CONTAINER_NAME);

const upload = multer({ storage: multer.memoryStorage() });

// Add this after your middleware section and before your routes
app.use(async (req, res, next) => {
  try {
    const token = req.cookies.token;
    if (token) {
      const decoded = jwt.verify(token, process.env.JWT_SECRET);
      const user = await User.findById(decoded._id);
      req.user = user;
    }
  } catch (error) {
    console.error("Auth middleware error:", error);
  }
  next();
});

// Upload to Azure — inputs are CSV only; pipeline produces outputs as XLSX (Lillybelle + ARCEP)
app.post("/upload", authMiddleware, upload.single("file"), async (req, res) => {
  if (!req.file) return res.status(400).json({ message: "No file uploaded." });
  const originalFileName = req.file.originalname;
  if (!originalFileName.toLowerCase().endsWith(".csv")) {
    return res.status(400).json({ message: "Only CSV input files are accepted. Outputs are produced as XLSX." });
  }
  try {
    const userId = req.user._id;
    
    // Extract reference number from the file name if possible
    // Input pattern: e.g. "RawData_ExportToCsv_20250204200349.csv"
    let fileReference = '';
    const referenceMatch = originalFileName.match(/\d+/);
    if (referenceMatch) {
      fileReference = referenceMatch[0]; // Use the first sequence of numbers found
    } else {
      // Fallback to random number if no reference can be extracted
      fileReference = Math.floor(Math.random() * 1000000).toString();
    }
    
    console.log(`File Reference: ${fileReference}`);
    
    // 1. Upload original file to Azure INPUT folder (container: ${CONTAINER_NAME}, path: INPUT/<filename>)
    const inputPath = `INPUT/${originalFileName}`;
    const inputBlobClient = containerClient.getBlockBlobClient(inputPath);
    try {
      await inputBlobClient.uploadData(req.file.buffer, {
        blobHTTPHeaders: { blobContentType: req.file.mimetype || "text/csv" }
      });
    } catch (azureErr) {
      console.error("❌ Azure upload failed:", azureErr.message || azureErr);
      return res.status(503).json({
        success: false,
        message: "Failed to upload file to storage. Check Azure connection and container.",
        error: azureErr.message
      });
    }
    const exists = await inputBlobClient.exists();
    if (!exists) {
      console.error("❌ Upload reported success but blob not found at " + inputPath);
      return res.status(503).json({
        success: false,
        message: "File upload could not be verified in storage.",
        inputPath
      });
    }
    console.log(`✅ File uploaded to Azure INPUT: ${inputPath} (container: ${CONTAINER_NAME})`);
    
    // Create placeholders for expected output files (outputs are always .xlsx, not .csv)
    // These will be updated when the files are detected in Azure storage
    
    // Lillybelle output (XLSX)
    const lillybelleFileName = `lillybelle_output_${fileReference}.xlsx`;
    const lillybelleFile = new File({
      userId,
      fileName: lillybelleFileName,
      originalName: originalFileName,
      fileReference: fileReference,
      fileType: 'lillybelle'
    });
    await lillybelleFile.save();
    
    // ARCEP output (XLSX)
    const arcepFileName = `ARCEP_output_${fileReference}.xlsx`;
    const arcepFile = new File({
      userId,
      fileName: arcepFileName,
      originalName: originalFileName,
      fileReference: fileReference,
      fileType: 'arcep',
      relatedFiles: [lillybelleFile._id]
    });
    await arcepFile.save();
    
    // Update Lillybelle file to reference ARCEP file
    await File.findByIdAndUpdate(lillybelleFile._id, {
      $push: { relatedFiles: arcepFile._id }
    });
    
    // Return success response immediately after upload
    res.json({ 
      success: true, 
      message: "✅ File uploaded to Snowflake for processing. Output files will be available once processing is complete.",
      originalFileName: originalFileName,
      fileReference: fileReference,
      fileToken: lillybelleFile.fileToken
    });
    
  } catch (error) {
    console.error("❌ Upload Error:", error);
    res.status(500).json({ success: false, message: "❌ Error uploading file.", error: error.message });
  }
});

// Verify upload: check if input file is in Azure INPUT and if any output exists (for debugging)
app.get("/api/verify-upload/:reference", authMiddleware, async (req, res) => {
  try {
    const reference = req.params.reference;
    const inputMatches = [];
    const outputMatches = await findFilesInAzureByReference(reference);
    for await (const blob of containerClient.listBlobsFlat({ prefix: "INPUT/" })) {
      if (blob.name.includes(reference)) inputMatches.push(blob.name);
    }
    res.json({
      success: true,
      reference,
      container: CONTAINER_NAME,
      input: { found: inputMatches.length > 0, paths: inputMatches },
      output: { found: outputMatches.length > 0, paths: outputMatches.map((f) => f.name) }
    });
  } catch (e) {
    console.error("Verify upload error:", e);
    res.status(500).json({ success: false, message: e.message });
  }
});

// Check Azure for processed files (can be called from client via polling)
app.get("/check-processed-files/:fileReference", authMiddleware, async (req, res) => {
  try {
    const fileReference = req.params.fileReference;
    const userId = req.user._id;
    
    console.log(`Checking for processed files with reference: ${fileReference}`);
    
    // Find database records for expected files
    const files = await File.find({ 
      userId: userId,
      fileReference: fileReference
    });
    
    if (!files || files.length === 0) {
      console.log(`No files found with reference: ${fileReference}`);
      return res.status(404).json({ 
        success: false, 
        message: "No files found with that reference number"
      });
    }
    
    console.log(`Found ${files.length} file records with reference: ${fileReference}`);
    
    // Find actual files in Azure by reference
    const azureFiles = await findFilesInAzureByReference(fileReference);
    
    // Process results and update database records
    const results = [];
    
    for (const file of files) {
      let exists = false;
      let azureFile = null;
      
      // Check if there's a matching Azure file
      for (const af of azureFiles) {
        if (af.name.toLowerCase().includes(file.fileName.toLowerCase()) ||
            (af.name.toLowerCase().includes('lillybelle') && file.fileType === 'lillybelle') ||
            (af.name.toLowerCase().includes('arcep') && file.fileType === 'arcep')) {
          exists = true;
          azureFile = af;
          break;
        }
      }
      
      // Update database record if file exists in Azure
      if (exists && azureFile) {
        await File.findByIdAndUpdate(file._id, {
          azurePath: azureFile.path,
          isReady: true
        });
      }
      
      results.push({
        fileName: file.fileName,
        fileType: file.fileType,
        fileToken: file.fileToken,
        exists: exists,
        azurePath: azureFile ? azureFile.path : null,
        properties: azureFile ? {
          contentLength: azureFile.contentLength,
          lastModified: azureFile.lastModified
        } : null
      });
    }
    
    res.json({
      success: true,
      fileReference: fileReference,
      results: results,
      azureFiles: azureFiles.map(f => f.name)
    });
    
  } catch (error) {
    console.error("❌ Error checking processed files:", error);
    res.status(500).json({ 
      success: false, 
      message: "Error checking processed files", 
      error: error.message 
    });
  }
});

// List all files in a specified Azure folder (INPUT or OUTPUT) — same container, prefix INPUT/ or OUTPUT/
app.get("/azure-files/:folder/:prefix?", authMiddleware, async (req, res) => {
  try {
    const folder = req.params.folder.toUpperCase();
    const extraPrefix = req.params.prefix || "";
    
    if (folder !== "INPUT" && folder !== "OUTPUT") {
      return res.status(400).json({ 
        success: false, 
        message: "Invalid folder. Must be INPUT or OUTPUT."
      });
    }
    
    const files = [];
    const prefix = folder + "/" + extraPrefix;
    const options = { prefix };
    
    for await (const blob of containerClient.listBlobsFlat(options)) {
      files.push({
        name: blob.name,
        contentLength: blob.properties.contentLength,
        lastModified: blob.properties.lastModified
      });
    }
    
    res.json({ success: true, files: files });
    
  } catch (error) {
    console.error("❌ Error listing Azure files:", error);
    res.status(500).json({ 
      success: false, 
      message: "Error listing Azure files", 
      error: error.message 
    });
  }
});

// Download Azure File
app.get("/download/:fileToken", authMiddleware, async (req, res) => {
  try {
    const fileToken = req.params.fileToken;
    
    // Find the file record using the token
    const fileRecord = await File.findOne({ fileToken });
    
    if (!fileRecord) {
      console.log(`❌ File not found in database for token: ${fileToken}`);
      return res.status(404).json({ success: false, message: "File not found" });
    }
    
    // Check if user is authorized to download this file
    if (fileRecord.userId.toString() !== req.user._id.toString()) {
      console.log(`❌ Unauthorized download attempt for token: ${fileToken}`);
      return res.status(403).json({ success: false, message: "Not authorized" });
    }
    
    const fileName = fileRecord.fileName;
    let filePath = fileRecord.azurePath || `OUTPUT/${fileName}`;
    
    console.log(`⬇️ Download requested for: ${filePath} (token: ${fileToken})`);
    
    // If azurePath is not set or points to .csv.xlsx, resolve to prefer .xlsx when both exist
    const shouldResolvePath = !fileRecord.azurePath || fileRecord.azurePath.toLowerCase().includes(".csv.xlsx");
    if (shouldResolvePath) {
      if (!fileRecord.azurePath) {
        console.log(`No azure path set, searching for matching files with reference: ${fileRecord.fileReference}`);
      } else {
        console.log(`Resolving path (prefer .xlsx over .csv.xlsx) for reference: ${fileRecord.fileReference}`);
      }
      const azureFiles = await findFilesInAzureByReference(fileRecord.fileReference);
      if (azureFiles.length > 0) {
        const matchedFile = pickBestAzureMatch(azureFiles, fileRecord.fileType);
        if (matchedFile) {
          console.log(`Found matching file in Azure: ${matchedFile.path}`);
          filePath = matchedFile.path;
          await File.findByIdAndUpdate(fileRecord._id, {
            azurePath: matchedFile.path,
            isReady: true
          });
        }
      }
    }
    
    try {
      // Try the download with the determined path
      let blockBlobClient = containerClient.getBlockBlobClient(filePath);
      let exists = await blockBlobClient.exists();
      
      if (!exists && !filePath.startsWith('OUTPUT/')) {
        // Try prepending OUTPUT/ if not already present
        filePath = `OUTPUT/${filePath}`;
        const altBlobClient = containerClient.getBlockBlobClient(filePath);
        exists = await altBlobClient.exists();
        
        if (exists) {
          console.log(`File found at corrected path: ${filePath}`);
          blockBlobClient = altBlobClient;
        }
      }
      
    if (!exists) {
        console.log(`❌ File not found at path: ${filePath}`);
        
        // Try case-insensitive search as a last resort
        console.log(`❓ Exact file match not found, checking for case-insensitive match...`);
        
        // If exact match not found, list all blobs and look for case-insensitive match
        let foundBlob = null;
        
        // List all blobs in the container with the OUTPUT/ prefix
        for await (const blob of containerClient.listBlobsFlat({ prefix: 'OUTPUT/' })) {
          // Check for case-insensitive match
          if (blob.name.toLowerCase() === filePath.toLowerCase() || 
              blob.name.toLowerCase().includes(fileName.toLowerCase())) {
            console.log(`✅ Found possible match: ${blob.name}`);
            foundBlob = blob.name;
            break;
          }
        }
        
        if (foundBlob) {
          console.log(`🔍 Found matching blob: ${foundBlob}`);
          
          // Use the found blob
          filePath = foundBlob;
          blockBlobClient = containerClient.getBlockBlobClient(foundBlob);
          exists = true;
          
          // Update the database record
          await File.findByIdAndUpdate(fileRecord._id, {
            azurePath: foundBlob,
            isReady: true
          });
        } else {
          console.log(`❌ No matching file found in Azure storage for: ${filePath}`);
          return res.status(404).json({ 
            success: false, 
            message: "File not found in OUTPUT storage. Processing may not be complete." 
          });
        }
      }
      
      // Outputs are always XLSX (Lillybelle/ARCEP); input was CSV
      const isOutputXlsx = fileRecord.fileType === "lillybelle" || fileRecord.fileType === "arcep"
        || filePath.toLowerCase().endsWith(".xlsx");
      const contentType = isOutputXlsx
        ? "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        : "text/csv";
      
      console.log(`✅ Downloading file: ${filePath}`);

    // Download the file
      const downloadResponse = await blockBlobClient.download(0);
      
      res.setHeader("Content-Disposition", `attachment; filename="${fileName}"`);
      res.setHeader("Content-Type", contentType);
      
      // Pipe the blob content directly to the response
    downloadResponse.readableStreamBody.pipe(res);
    } catch (downloadError) {
      console.error(`❌ Azure download error for ${filePath}:`, downloadError);
      return res.status(500).json({ 
        success: false, 
        message: "Error downloading file from Azure storage", 
        error: downloadError.message 
      });
    }
  } catch (error) {
    console.error("❌ Download Error:", error);
    res.status(500).json({ success: false, message: "❌ Error downloading file.", error: error.message });
  }
});

// Signup
app.post("/signup", async (req, res) => {
  try {
    const { firstName, lastName, country, phone, email, password, confirmPassword } = req.body;

    if (!firstName || !lastName || !country || !phone || !email || !password || !confirmPassword) {
      return res.status(400).json({ success: false, message: "All fields are required" });
    }

    if (password !== confirmPassword) {
      return res.status(400).json({ success: false, message: "Passwords do not match" });
    }

    const existingUser = await User.findOne({ email });
    if (existingUser) {
      return res.status(400).json({ success: false, message: "Email already registered" });
    }

    const newUser = new User({ firstName, lastName, country, phone, email, password });

    await newUser.save();

    console.log("✅ New user created:", newUser); // Debug log

    return res.status(201).json({ success: true, message: "Account created successfully" });
  } catch (error) {
    console.error("❌ Signup Error:", error.message, error);
    return res.status(500).json({ success: false, message: "Server error occurred" });
  }
});


// Login with JWT
app.post("/login", async (req, res) => {
  try {
    const { email, password } = req.body;
    
    // Find user and include password field
    const user = await User.findOne({ email }).select('+password');
    if (!user) {
      return res.status(400).json({ 
        success: false,
        message: "No account found with this email." 
      });
    }

    // Compare password
    const isMatch = await bcrypt.compare(password, user.password);
    if (!isMatch) {
      return res.status(400).json({ 
        success: false,
        message: "Incorrect email or password" 
      });
    }

    // Generate JWT token
    const token = jwt.sign(
      { _id: user._id, email: user.email },
      process.env.JWT_SECRET || "e0994fb02524c80f839de457da95697811aa51dea6ed56f49b656e66094fb8c302517248cebcf024162beeb90bbdaebe75882ec7dd5d29bf689b750a8b8aa77f",
      { expiresIn: "1d" }
    );

    // Set cookie
    res.cookie("token", token, {
      httpOnly: true,
      secure: process.env.NODE_ENV === "production",
      sameSite: "Strict",
      maxAge: 24 * 60 * 60 * 1000 // 1 day
    });

    // Send success response with redirect
    res.json({ 
      success: true,
      redirect: "/"
    });

  } catch (error) {
    console.error("❌ Login Error:", error);
    res.status(500).json({ 
      success: false, 
      message: "Server error occurred" 
    });
  }
});

// Logout
app.post("/logout", (req, res) => {
  res.clearCookie("token");
  res.redirect("/login");
});

// Page Views
app.get("/", (req, res) => {
    res.render("pages/home", { user: req.user || null });
  });
  

app.get("/services", authMiddleware, async (req, res) => {
  try {
      const azureFiles = [];
      for await (const blob of containerClient.listBlobsFlat({ prefix: "OUTPUT/" })) {
        azureFiles.push(blob.name); // e.g., OUTPUT/processed_data.csv
      }
  
      const userFiles = await File.find({ userId: req.user._id }).sort({ uploadedAt: -1 });
      res.render("pages/services", {
        files: userFiles,
        azureFiles,
        user: req.user
      });
    } catch (error) {
      console.error("❌ Error loading services:", error);
      res.status(500).send("Erreur interne du serveur");
    }
  });
  
app.get("/file-history", authMiddleware, async (req, res) => {
    try {
    // Get all files for the user
    const files = await File.find({ userId: req.user._id || null }).sort({ uploadedAt: -1 });
    
    // Group files by fileReference
    const groupedFiles = {};
    files.forEach(file => {
      if (!groupedFiles[file.fileReference]) {
        groupedFiles[file.fileReference] = {
          reference: file.fileReference,
          originalName: file.originalName,
          uploadedAt: file.uploadedAt,
          files: []
        };
      }
      
      groupedFiles[file.fileReference].files.push({
        id: file._id,
        fileName: file.fileName,
        fileType: file.fileType,
        fileToken: file.fileToken,
        uploadedAt: file.uploadedAt
      });
    });
    
    res.json(Object.values(groupedFiles));
    } catch (error) {
      console.error("❌ Error fetching file history:", error);
    res.status(500).json({ message: "Error loading file history" });
    }
  });
  
app.get("/about", (req, res) => res.render("pages/about", { user: req.user || null }));
app.get("/signup", (req, res) => res.render("pages/signup"));
app.get("/login", (req, res) => res.render("pages/login"));
app.get("/contact", (req, res) => res.render("pages/contact", { user: req.user || null }));

// Handle contact form submissions
app.post("/contact", async (req, res) => {
  try {
    const { name, email, message } = req.body;
    
    // Create new contact record
    const newContact = new Contact({
      name,
      email,
      message
    });
    
    // Save to database
    await newContact.save();
    
    // Set flash message for success
    req.flash("success_msg", "Your message has been sent successfully!");
    
    // Redirect back to contact page without query parameters
    res.redirect("/contact");
  } catch (error) {
    console.error("❌ Contact form error:", error);
    
    // Set flash message for error
    req.flash("error_msg", "An error occurred while sending your message.");
    
    // Redirect back to contact page without query parameters
    res.redirect("/contact");
  }
});

// Get file info by token
app.get("/file-info/:fileToken", authMiddleware, async (req, res) => {
  try {
    const fileToken = req.params.fileToken;
    
    // Find the file record using the token
    const fileRecord = await File.findOne({ fileToken });
    
    if (!fileRecord) {
      console.log(`❌ File not found in database for token: ${fileToken}`);
      return res.status(404).json({ 
        success: false, 
        message: "File not found" 
      });
    }
    
    // Check if user is authorized to access this file info
    if (fileRecord.userId.toString() !== req.user._id.toString()) {
      console.log(`❌ Unauthorized file info request for token: ${fileToken}`);
      return res.status(403).json({ 
        success: false, 
        message: "Not authorized" 
      });
    }
    
    // Return file information
    res.json({
      success: true,
      fileName: fileRecord.fileName,
      originalName: fileRecord.originalName,
      fileType: fileRecord.fileType,
      fileReference: fileRecord.fileReference,
      uploadedAt: fileRecord.uploadedAt
    });
  } catch (error) {
    console.error("❌ Error getting file info:", error);
    res.status(500).json({ 
      success: false, 
      message: "Error retrieving file information", 
      error: error.message 
    });
  }
});

// Check if a specific blob exists in Azure
app.get("/check-azure-blob/:container/:blobPath", authMiddleware, async (req, res) => {
  try {
    const containerName = req.params.container;
    const blobPath = req.params.blobPath;
    
    if (containerName !== "INPUT" && containerName !== "OUTPUT") {
      return res.status(400).json({ 
        success: false, 
        message: "Invalid container name. Must be INPUT or OUTPUT."
      });
    }
    
    console.log(`Checking existence of blob: ${containerName}/${blobPath}`);
    
    // If the blob path starts with the container name, remove it
    const normalizedBlobPath = blobPath.startsWith(`${containerName}/`) 
      ? blobPath.substring(containerName.length + 1) 
      : blobPath;
    
    // Full path with container prefix
    const fullBlobPath = `${containerName}/${normalizedBlobPath}`;
    
    const blobClient = containerClient.getBlockBlobClient(fullBlobPath);
    
    // Check if the blob exists and get properties
    const exists = await blobClient.exists();
    let properties = null;
    
    if (exists) {
      try {
        properties = await blobClient.getProperties();
      } catch (propError) {
        console.error(`Error getting properties for blob ${fullBlobPath}:`, propError);
      }
    }
    
    console.log(`Blob ${fullBlobPath} exists: ${exists}`);
    
    res.json({
      success: true,
      exists: exists,
      blobPath: fullBlobPath,
      properties: properties ? {
        contentType: properties.contentType,
        contentLength: properties.contentLength,
        lastModified: properties.lastModified,
        createdOn: properties.createdOn
      } : null
    });
    
  } catch (error) {
    console.error("❌ Error checking blob:", error);
    res.status(500).json({ 
      success: false, 
      message: "Error checking blob existence", 
      error: error.message 
    });
  }
});

// Direct diagnostic blob check and download
app.get("/direct-blob-check/:fileName", authMiddleware, async (req, res) => {
  try {
    const fileName = req.params.fileName;
    const filePath = `OUTPUT/${fileName}`;
    
    console.log(`🔍 Checking direct blob access: ${filePath}`);
    
    const blockBlobClient = containerClient.getBlockBlobClient(filePath);
    
    // Check if blob exists
    const exists = await blockBlobClient.exists();
    console.log(`✅ File exists check: ${exists ? 'YES' : 'NO'} for ${filePath}`);
    
    if (!exists) {
      return res.status(404).json({ 
        success: false, 
        message: "File not found in Azure storage",
        filePath: filePath 
      });
    }
    
    // Get properties
    try {
      const properties = await blockBlobClient.getProperties();
      console.log(`File properties:`, {
        contentType: properties.contentType,
        contentLength: properties.contentLength,
        lastModified: properties.lastModified
      });
      
      return res.json({
        success: true,
        message: "File exists in Azure storage",
        filePath: filePath,
        properties: {
          contentType: properties.contentType,
          contentLength: properties.contentLength,
          lastModified: properties.lastModified
        }
      });
    } catch (propError) {
      console.error(`❌ Error getting properties for ${filePath}:`, propError);
      return res.status(500).json({
        success: false,
        message: "Error retrieving file properties",
        error: propError.message
      });
    }
  } catch (error) {
    console.error("❌ Direct blob check error:", error);
    res.status(500).json({ 
      success: false, 
      message: "Error checking blob", 
      error: error.message 
    });
  }
});

// Direct file download by name
app.get("/direct-download/:fileName", authMiddleware, async (req, res) => {
  try {
    const fileName = req.params.fileName;
    const filePath = `OUTPUT/${fileName}`;
    
    console.log(`⬇️ Direct download request for: ${filePath}`);
    
    const blockBlobClient = containerClient.getBlockBlobClient(filePath);
    
    // Check if blob exists
    const exists = await blockBlobClient.exists();
    console.log(`File exists check: ${exists ? 'YES' : 'NO'} for ${filePath}`);
    
    if (!exists) {
      return res.status(404).json({ 
        success: false, 
        message: "File not found in Azure storage",
        filePath: filePath 
      });
    }

    try {
      // Set correct content type based on file extension
      const contentType = fileName.endsWith('.xlsx') 
        ? 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        : 'text/csv';

      // Download the file
      const downloadResponse = await blockBlobClient.download(0);
      
      console.log(`✅ Successfully downloading file: ${filePath}`);
      
      res.setHeader("Content-Disposition", `attachment; filename="${fileName}"`);
      res.setHeader("Content-Type", contentType);
      
      // Pipe the blob content directly to the response
      downloadResponse.readableStreamBody.pipe(res);
    } catch (downloadError) {
      console.error(`❌ Azure download error for ${filePath}:`, downloadError);
      return res.status(500).json({ 
        success: false, 
        message: "Error downloading file from Azure storage", 
        error: downloadError.message 
      });
    }
  } catch (error) {
    console.error("❌ Download Error:", error);
    res.status(500).json({ success: false, message: "❌ Error downloading file.", error: error.message });
  }
});

// Helper function to find matching files in Azure by reference
async function findFilesInAzureByReference(reference) {
  console.log(`🔍 Searching for files with reference: ${reference} in Azure OUTPUT/`);
  
  const matches = [];
  
  try {
    // List all blobs in the OUTPUT container
    for await (const blob of containerClient.listBlobsFlat({ prefix: 'OUTPUT/' })) {
      // Check if the blob name contains the reference
      if (blob.name.includes(reference)) {
        console.log(`✅ Found matching file: ${blob.name}`);
        
        // Get properties
        try {
          const properties = await containerClient.getBlockBlobClient(blob.name).getProperties();
          
          matches.push({
            name: blob.name,
            path: blob.name,
            reference: reference,
            contentType: properties.contentType,
            contentLength: properties.contentLength,
            lastModified: properties.lastModified
          });
        } catch (propError) {
          console.error(`Error getting properties for ${blob.name}:`, propError);
          
          // Add with limited info
          matches.push({
            name: blob.name,
            path: blob.name,
            reference: reference
          });
        }
      }
    }
    
    console.log(`Found ${matches.length} files matching reference: ${reference}`);
    return matches;
  } catch (error) {
    console.error(`❌ Error searching for files by reference: ${reference}`, error);
    return [];
  }
}

// Outputs are XLSX only (inputs are CSV). Prefer true .xlsx over misnamed .csv.xlsx when both exist.
function pickBestAzureMatch(azureFiles, fileType) {
  if (!azureFiles || azureFiles.length === 0) return null;
  const key = fileType === "lillybelle" ? "lillybelle" : "arcep";
  const candidates = azureFiles.filter(af => af.name.toLowerCase().includes(key));
  if (candidates.length === 0) return null;
  const preferXlsx = candidates.find(af => {
    const n = af.name.toLowerCase();
    return n.endsWith(".xlsx") && !n.includes(".csv.xlsx");
  });
  if (preferXlsx) {
    return preferXlsx;
  }
  return candidates[0];
}

// Find files by reference in Azure storage
app.get("/find-files-by-reference/:reference", authMiddleware, async (req, res) => {
  try {
    const reference = req.params.reference;
    
    // Find database records for this reference
    const dbFiles = await File.find({ fileReference: reference });
    
    // Find actual files in Azure
    const azureFiles = await findFilesInAzureByReference(reference);
    
    // Update database if files are found (prefer .xlsx over .csv.xlsx per file type)
    if (azureFiles.length > 0 && dbFiles.length > 0) {
      for (const dbFile of dbFiles) {
        const best = pickBestAzureMatch(azureFiles, dbFile.fileType);
        if (best) {
          console.log(`Updating file record ${dbFile._id} with actual Azure path: ${best.path}`);
          await File.findByIdAndUpdate(dbFile._id, { azurePath: best.path });
        }
      }
    }
    
    res.json({
      success: true,
      reference: reference,
      dbFiles: dbFiles.map(f => ({
        id: f._id,
        fileName: f.fileName,
        fileType: f.fileType,
        fileToken: f.fileToken,
        azurePath: f.azurePath
      })),
      azureFiles: azureFiles
    });
  } catch (error) {
    console.error(`❌ Error finding files by reference: ${error}`);
    res.status(500).json({
      success: false,
      message: "Error finding files by reference",
      error: error.message
    });
  }
});

// API: List all output files for the current user
app.get("/api/output-files", authMiddleware, async (req, res) => {
  try {
    // Find all output files for the user (Lillybelle and ARCEP)
    const files = await File.find({
      userId: req.user._id,
      fileType: { $in: ["lillybelle", "arcep"] }
    }).sort({ uploadedAt: -1 });

    // Return minimal info for dropdown
    res.json({
      success: true,
      files: files.map(f => ({
        id: f._id,
        fileName: f.fileName,
        fileType: f.fileType,
        fileReference: f.fileReference,
        uploadedAt: f.uploadedAt,
        azurePath: f.azurePath || null
      }))
    });
  } catch (error) {
    console.error("❌ Error listing output files:", error);
    res.status(500).json({ success: false, message: "Error listing output files" });
  }
});

// Helper: read stream into Buffer
function streamToBuffer(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (chunk) => chunks.push(chunk));
    stream.on("end", () => resolve(Buffer.concat(chunks)));
    stream.on("error", reject);
  });
}

// Parse Lillybelle Excel and return data for the 4 Analysis charts (real data from file)
function parseLillybelleForCharts(buffer) {
  const result = {
    chart1: { labels: ["Vert", "Jaune", "Orange", "Rouge"], togocel: [45, 0, 0, 3.5], moov: [29, 4, 4, 11] },
    chart2: { labels: ["Point 1", "Point 2", "Point 3"], togocel: [92, 94, 96], moov: [70, 72, 75] },
    chart3: {
      labels: ["SV1", "SV2", "SV3", "SV4", "NW1_3G", "NW2_3G", "TD1_3G", "TD2_3G", "TD3_3G", "TD4_3G", "NW1_4G", "NW2_4G", "TD1_4G", "TD2_4G", "TD3_4G", "TD4_4G"],
      togocel: [100, 100, 100, 0, 95, 95, 90, 60, 5, 20, 95, 95, 85, 90, 95, 100],
      moov: [85, 88, 90, 0, 20, 60, 90, 85, 95, 90, 25, 28, 85, 90, 100, 100]
    },
    chart4: { labels: ["Voix", "Données 3G", "Données 4G"], togocel: [100, 90, 95], moov: [50, 67, 72] }
  };
  const num = (v) => {
    if (v == null || v === "") return NaN;
    if (typeof v === "number") return isNaN(v) ? NaN : v;
    const s = String(v).replace(/,/g, ".").replace(/%/g, "").trim();
    const n = parseFloat(s);
    return isNaN(n) ? NaN : (n <= 1 && n >= 0 && !String(v).includes("%") ? n * 100 : n);
  };
  const clamp = (x) => Math.min(120, Math.max(0, Math.round(Number(x))));
  const radarMap = [
    { keys: ["SV1", "SetupTime"], idx: 0 },
    { keys: ["SV2", "voix réussis"], idx: 1 },
    { keys: ["SV3", "MOS"], idx: 2 },
    { keys: ["SV4", "voix drop"], idx: 3 },
    { keys: ["NW1_3G", "NW1", "3G", "Navigation Web Fail"], idx: 4 },
    { keys: ["NW2_3G", "NW2", "3G", "Chargement"], idx: 5 },
    { keys: ["TD1_3G", "Débit UP", "3G", "TD1"], idx: 6 },
    { keys: ["TD2_3G", "Débit DOWN", "3G", "TD2"], idx: 7 },
    { keys: ["TD3_3G", "Transferts UP", "3G", "TD3"], idx: 8 },
    { keys: ["TD4_3G", "Transferts DOWN", "3G", "TD4"], idx: 9 },
    { keys: ["NW1_4G", "NW1", "4G"], idx: 10 },
    { keys: ["NW2_4G", "NW2", "4G"], idx: 11 },
    { keys: ["TD1_4G", "Débit UP", "4G"], idx: 12 },
    { keys: ["TD2_4G", "Débit DOWN", "4G"], idx: 13 },
    { keys: ["TD3_4G", "Transferts UP", "4G"], idx: 14 },
    { keys: ["TD4_4G", "Transferts DOWN", "4G"], idx: 15 }
  ];
  // Find all POINT columns and optional locality names (row 1 or header)
  function getPointColumns(header, row1) {
    const indices = [];
    const re = /^POINT\s*(\d+)$/i;
    for (let c = 0; c < header.length; c++) {
      const h = (header[c] || "").trim();
      const m = h.match(re);
      if (m) indices.push({ col: c, num: parseInt(m[1], 10) });
    }
    indices.sort((a, b) => a.col - b.col);
    const labels = indices.map((p, i) => {
      const fromRow1 = row1 && row1[p.col] != null && String(row1[p.col]).trim() !== "";
      const name = fromRow1 ? String(row1[p.col]).trim() : (header[p.col] || "").trim();
      if (name && !/^POINT\s*\d+$/i.test(name) && isNaN(parseFloat(name))) return name;
      return "Point " + (i + 1);
    });
    return { pointCols: indices.map(p => p.col), labels };
  }
  try {
    const wb = XLSX.read(buffer, { type: "buffer", cellDates: false });
    const sheets = wb.SheetNames || [];
    const avg = (a) => (a.length ? a.reduce((s, x) => s + (isNaN(x) ? 0 : x), 0) / a.filter(x => !isNaN(x)).length : null);
    let chart2LabelsSet = false;
    for (let si = 0; si < sheets.length; si++) {
      const ws = wb.Sheets[sheets[si]];
      const rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });
      if (!rows || rows.length < 2) continue;
      const header = rows[0].map(h => (h != null ? String(h).trim() : ""));
      const row1 = rows[1];
      const { pointCols, labels: pointLabels } = getPointColumns(header, row1);
      if (!chart2LabelsSet && pointCols.length > 0) {
        result.chart2.labels = pointLabels;
        result.chart2.togocel = pointCols.map(() => 90);
        result.chart2.moov = pointCols.map(() => 70);
        chart2LabelsSet = true;
      }
      const ensembleIdx = header.findIndex(h => h && h.toUpperCase().includes("ENSEMBLE"));
      const ensIdx = ensembleIdx >= 0 ? ensembleIdx : header.length - 1;
      const isTogocel = si === 0 || (sheets[si] && String(sheets[si]).toLowerCase().includes("togo"));
      const arr = isTogocel ? result.chart3.togocel : result.chart3.moov;
      const arr2 = isTogocel ? result.chart2.togocel : result.chart2.moov;
      const voicePct = [], data3G = [], data4G = [];
      let okLocaliteRow = null;
      for (let r = 1; r < rows.length; r++) {
        const row = rows[r];
        const calc = (row[0] != null ? String(row[0]).trim() : "").toLowerCase();
        if (!calc) continue;
        const val = num(row[ensIdx]);
        if (calc.includes("sv") || calc.includes("voix") || calc.includes("setup") || calc.includes("mos")) voicePct.push(val);
        if (calc.includes("3g")) data3G.push(val);
        if (calc.includes("4g")) data4G.push(val);
        if (!isNaN(val)) {
          for (const { keys, idx } of radarMap) {
            if (keys.some(k => calc.includes(k.toLowerCase()))) {
              arr[idx] = clamp(val);
              break;
            }
          }
          if (pointCols.length > 0 && (calc.includes("ok") || calc.includes("réussis") || calc.includes("sv2") || calc.includes("voix") || calc.includes("%"))) {
            const values = pointCols.map(c => num(row[c])).filter(v => !isNaN(v));
            if (values.length === pointCols.length) {
              if (calc.includes("ok") || calc.includes("réussis") || calc.includes("sv2")) okLocaliteRow = values;
              else if (!okLocaliteRow) okLocaliteRow = values;
            }
          }
        }
      }
      if (okLocaliteRow && okLocaliteRow.length === arr2.length) {
        okLocaliteRow.forEach((v, i) => { if (i < arr2.length) arr2[i] = clamp(v); });
      }
      const chart4arr = isTogocel ? result.chart4.togocel : result.chart4.moov;
      const vAvg = avg(voicePct), g3 = avg(data3G), g4 = avg(data4G);
      if (vAvg != null) chart4arr[0] = clamp(vAvg);
      if (g3 != null) chart4arr[1] = clamp(g3);
      if (g4 != null) chart4arr[2] = clamp(g4);
    }
  } catch (e) {
    console.warn("Lillybelle parse warning, using defaults:", e.message);
  }
  return result;
}

// API: Get chart data for a Lillybelle file (by reference) – used by Analysis tab
app.get("/api/lillybelle-chart-data/:reference", authMiddleware, async (req, res) => {
  try {
    const reference = req.params.reference;
    const fileRecord = await File.findOne({
      userId: req.user._id,
      fileReference: reference,
      fileType: "lillybelle"
    });
    if (!fileRecord) {
      return res.status(404).json({ success: false, message: "Lillybelle file not found for this reference" });
    }
    let filePath = fileRecord.azurePath || `OUTPUT/${fileRecord.fileName}`;
    const shouldResolve = !fileRecord.azurePath || fileRecord.azurePath.toLowerCase().includes(".csv.xlsx");
    if (shouldResolve) {
      const azureFiles = await findFilesInAzureByReference(reference);
      const match = pickBestAzureMatch(azureFiles, "lillybelle");
      if (match) {
        filePath = match.path;
        await File.findByIdAndUpdate(fileRecord._id, { azurePath: match.path });
      }
    }
    const blockBlobClient = containerClient.getBlockBlobClient(filePath);
    const exists = await blockBlobClient.exists();
    if (!exists) {
      return res.status(404).json({ success: false, message: "File not found in storage" });
    }
    const downloadResponse = await blockBlobClient.download(0);
    const buffer = await streamToBuffer(downloadResponse.readableStreamBody);
    const chartData = parseLillybelleForCharts(buffer);
    res.json({ success: true, chartData });
  } catch (error) {
    console.error("❌ Error loading Lillybelle chart data:", error);
    res.status(500).json({ success: false, message: "Error loading chart data", error: error.message });
  }
});

// Delete file from history
app.delete("/delete-file/:fileId", authMiddleware, async (req, res) => {
  try {
    const fileId = req.params.fileId;
    
    // Find the file record
    const file = await File.findById(fileId);
    
    if (!file) {
      return res.status(404).json({ success: false, message: "File not found" });
    }
    
    // Check if user is authorized to delete this file
    if (file.userId.toString() !== req.user._id.toString()) {
      return res.status(403).json({ success: false, message: "Not authorized" });
    }
    
    // Delete the file record from database
    await File.findByIdAndDelete(fileId);
    
    res.json({ success: true, message: "File deleted successfully" });
  } catch (error) {
    console.error("❌ Error deleting file:", error);
    res.status(500).json({ success: false, message: "Error deleting file" });
  }
});

const PORT = process.env.PORT || 3000;
let server;

mongoose.connection.once("open", () => {
  server = app.listen(PORT, () => console.log(`🚀 Server running on http://localhost:${PORT}`));
  // Graceful shutdown so nodemon restarts don't leave port in use
  function shutdown() {
    if (server) {
      server.close(() => process.exit(0));
      setTimeout(() => process.exit(1), 5000);
    } else process.exit(0);
  }
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
});