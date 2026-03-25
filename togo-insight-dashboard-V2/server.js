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
  return mongoose.connect(process.env.MONGO_URI, mongoOptions)
    .then(() => {})
    .catch(err => {
      throw err;
    });
}
connectMongo()
  .catch(() => {
    return new Promise((resolve) => setTimeout(resolve, 5000)).then(connectMongo);
  })
  .catch(() => {
    return new Promise((resolve) => setTimeout(resolve, 5000)).then(connectMongo);
  })
  .catch(() => {
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
    // auth failed silently
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
    
    // 1. Upload original file to Azure INPUT folder (container: ${CONTAINER_NAME}, path: INPUT/<filename>)
    const inputPath = `INPUT/${originalFileName}`;
    const inputBlobClient = containerClient.getBlockBlobClient(inputPath);
    try {
      await inputBlobClient.uploadData(req.file.buffer, {
        blobHTTPHeaders: { blobContentType: req.file.mimetype || "text/csv" }
      });
    } catch (azureErr) {
      return res.status(503).json({
        success: false,
        message: "Failed to upload file to storage. Check Azure connection and container.",
        error: azureErr.message
      });
    }
    const exists = await inputBlobClient.exists();
    if (!exists) {
      return res.status(503).json({
        success: false,
        message: "File upload could not be verified in storage.",
        inputPath
      });
    }
    
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
    res.status(500).json({ success: false, message: e.message });
  }
});

// Check Azure for processed files (can be called from client via polling)
app.get("/check-processed-files/:fileReference", authMiddleware, async (req, res) => {
  try {
    const fileReference = req.params.fileReference;
    const userId = req.user._id;
    
    // Find database records for expected files
    const files = await File.find({ 
      userId: userId,
      fileReference: fileReference
    });
    
    if (!files || files.length === 0) {
      return res.status(404).json({ 
        success: false, 
        message: "No files found with that reference number"
      });
    }
    
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
        syncOutputFileFromAzureToMongo(file._id).catch((err) =>
          console.error("syncOutputFileFromAzureToMongo (check-processed):", err.message)
        );
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
      return res.status(404).json({ success: false, message: "File not found" });
    }
    
    // Check if user is authorized to download this file
    if (fileRecord.userId.toString() !== req.user._id.toString()) {
      return res.status(403).json({ success: false, message: "Not authorized" });
    }
    
    const fileName = fileRecord.fileName;
    let filePath = fileRecord.azurePath || `OUTPUT/${fileName}`;
    
    // If azurePath is not set or points to .csv.xlsx, resolve to prefer .xlsx when both exist
    const shouldResolvePath = !fileRecord.azurePath || fileRecord.azurePath.toLowerCase().includes(".csv.xlsx");
    if (shouldResolvePath) {
      const azureFiles = await findFilesInAzureByReference(fileRecord.fileReference);
      if (azureFiles.length > 0) {
        const matchedFile = pickBestAzureMatch(azureFiles, fileRecord.fileType);
        if (matchedFile) {
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
          blockBlobClient = altBlobClient;
        }
      }
      
    if (!exists) {
        // Try case-insensitive search as a last resort
        // If exact match not found, list all blobs and look for case-insensitive match
        let foundBlob = null;
        
        // List all blobs in the container with the OUTPUT/ prefix
        for await (const blob of containerClient.listBlobsFlat({ prefix: 'OUTPUT/' })) {
          // Check for case-insensitive match
          if (blob.name.toLowerCase() === filePath.toLowerCase() || 
              blob.name.toLowerCase().includes(fileName.toLowerCase())) {
            foundBlob = blob.name;
            break;
          }
        }
        
        if (foundBlob) {
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
          const fromMongo = await File.findById(fileRecord._id).select("+mongoFileBinary");
          if (fromMongo && fromMongo.mongoFileBinary && fromMongo.mongoFileBinary.length) {
            const isOutXlsx = fileRecord.fileType === "lillybelle" || fileRecord.fileType === "arcep"
              || fileName.toLowerCase().endsWith(".xlsx");
            const ct = isOutXlsx
              ? "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
              : "text/csv";
            res.setHeader("Content-Disposition", `attachment; filename="${fileName}"`);
            res.setHeader("Content-Type", ct);
            return res.send(fromMongo.mongoFileBinary);
          }
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

    // Download the file
      const downloadResponse = await blockBlobClient.download(0);
      
      res.setHeader("Content-Disposition", `attachment; filename="${fileName}"`);
      res.setHeader("Content-Type", contentType);
      
      // Pipe the blob content directly to the response
    downloadResponse.readableStreamBody.pipe(res);
    } catch (downloadError) {
      return res.status(500).json({ 
        success: false, 
        message: "Error downloading file from Azure storage", 
        error: downloadError.message 
      });
    }
  } catch (error) {
    res.status(500).json({ success: false, message: "❌ Error downloading file.", error: error.message });
  }
});

// Download status (fast pre-check) — used to avoid fetch+blob delays on the client
app.get("/download-status/:fileToken", authMiddleware, async (req, res) => {
  try {
    const fileToken = req.params.fileToken;

    const fileRecord = await File.findOne({ fileToken }).select("+mongoFileBinary");
    if (!fileRecord) {
      return res.status(404).json({ success: false, message: "File not found" });
    }

    if (fileRecord.userId.toString() !== req.user._id.toString()) {
      return res.status(403).json({ success: false, message: "Not authorized" });
    }

    // If stored in MongoDB already, download can start immediately
    if (fileRecord.mongoFileBinary && fileRecord.mongoFileBinary.length) {
      return res.json({ success: true, filename: fileRecord.fileName, source: "mongo" });
    }

    const fileName = fileRecord.fileName;
    let filePath = fileRecord.azurePath || `OUTPUT/${fileName}`;

    const shouldResolvePath =
      !fileRecord.azurePath || fileRecord.azurePath.toLowerCase().includes(".csv.xlsx");
    if (shouldResolvePath) {
      const azureFiles = await findFilesInAzureByReference(fileRecord.fileReference);
      if (azureFiles.length > 0) {
        const matchedFile = pickBestAzureMatch(azureFiles, fileRecord.fileType);
        if (matchedFile) {
          filePath = matchedFile.path;
          await File.findByIdAndUpdate(fileRecord._id, {
            azurePath: matchedFile.path,
            isReady: true
          });
        }
      }
    }

    let blockBlobClient = containerClient.getBlockBlobClient(filePath);
    let exists = await blockBlobClient.exists();

    if (!exists && !filePath.startsWith("OUTPUT/")) {
      filePath = `OUTPUT/${filePath}`;
      const altBlobClient = containerClient.getBlockBlobClient(filePath);
      exists = await altBlobClient.exists();
      if (exists) blockBlobClient = altBlobClient;
    }

    if (!exists) {
      // Case-insensitive search as last resort
      let foundBlob = null;
      for await (const blob of containerClient.listBlobsFlat({ prefix: "OUTPUT/" })) {
        if (
          blob.name.toLowerCase() === filePath.toLowerCase() ||
          blob.name.toLowerCase().includes(fileName.toLowerCase())
        ) {
          foundBlob = blob.name;
          break;
        }
      }

      if (foundBlob) {
        await File.findByIdAndUpdate(fileRecord._id, {
          azurePath: foundBlob,
          isReady: true
        });
        return res.json({ success: true, filename: fileName, source: "azure" });
      }

      return res.status(404).json({
        success: false,
        message: "File not found in OUTPUT storage. Processing may not be complete."
      });
    }

    return res.json({ success: true, filename: fileName, source: "azure" });
  } catch (error) {
    res.status(500).json({ success: false, message: "Error checking download status", error: error.message });
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

    return res.status(201).json({ success: true, message: "Account created successfully" });
  } catch (error) {
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
      return res.status(404).json({ 
        success: false, 
        message: "File not found" 
      });
    }
    
    // Check if user is authorized to access this file info
    if (fileRecord.userId.toString() !== req.user._id.toString()) {
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
      }
    }
    
    
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
    
    
    const blockBlobClient = containerClient.getBlockBlobClient(filePath);
    
    // Check if blob exists
    const exists = await blockBlobClient.exists();
    
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
      return res.status(500).json({
        success: false,
        message: "Error retrieving file properties",
        error: propError.message
      });
    }
  } catch (error) {
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
    
    
    const blockBlobClient = containerClient.getBlockBlobClient(filePath);
    
    // Check if blob exists
    const exists = await blockBlobClient.exists();
    
    if (!exists) {
      const decodedName = decodeURIComponent(fileName);
      const fileRecord = await File.findOne({
        userId: req.user._id,
        fileName: decodedName,
        fileType: { $in: ["lillybelle", "arcep"] }
      }).select("+mongoFileBinary");
      if (fileRecord && fileRecord.mongoFileBinary && fileRecord.mongoFileBinary.length) {
        const contentType = decodedName.endsWith(".xlsx")
          ? "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
          : "text/csv";
        res.setHeader("Content-Disposition", `attachment; filename="${decodedName}"`);
        res.setHeader("Content-Type", contentType);
        return res.send(fileRecord.mongoFileBinary);
      }
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
      
      
      res.setHeader("Content-Disposition", `attachment; filename="${fileName}"`);
      res.setHeader("Content-Type", contentType);
      
      // Pipe the blob content directly to the response
      downloadResponse.readableStreamBody.pipe(res);
    } catch (downloadError) {
      return res.status(500).json({ 
        success: false, 
        message: "Error downloading file from Azure storage", 
        error: downloadError.message 
      });
    }
  } catch (error) {
    res.status(500).json({ success: false, message: "❌ Error downloading file.", error: error.message });
  }
});

// Direct download status (fast pre-check) — used to avoid fetch+blob delays on the client
app.get("/direct-download-status/:fileName", authMiddleware, async (req, res) => {
  try {
    const rawName = req.params.fileName;
    const decodedName = decodeURIComponent(rawName);

    // Prefer Mongo binary if already cached
    const fileRecord = await File.findOne({
      userId: req.user._id,
      fileName: decodedName,
      fileType: { $in: ["lillybelle", "arcep"] }
    }).select("+mongoFileBinary");

    if (fileRecord && fileRecord.mongoFileBinary && fileRecord.mongoFileBinary.length) {
      return res.json({ success: true, filename: decodedName, source: "mongo" });
    }

    // Fallback to Azure existence check for the exact blob name
    const filePath = `OUTPUT/${decodedName}`;
    const blockBlobClient = containerClient.getBlockBlobClient(filePath);
    const exists = await blockBlobClient.exists();
    if (!exists) {
      return res.status(404).json({
        success: false,
        message: "File not found in Azure storage"
      });
    }

    return res.json({ success: true, filename: decodedName, source: "azure" });
  } catch (error) {
    res.status(500).json({ success: false, message: "Error checking direct download status", error: error.message });
  }
});

// Helper function to find matching files in Azure by reference
async function findFilesInAzureByReference(reference) {
  
  const matches = [];
  
  try {
    // List all blobs in the OUTPUT container
    for await (const blob of containerClient.listBlobsFlat({ prefix: 'OUTPUT/' })) {
      // Check if the blob name contains the reference
      if (blob.name.includes(reference)) {
        
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
          
          // Add with limited info
          matches.push({
            name: blob.name,
            path: blob.name,
            reference: reference
          });
        }
      }
    }
    
    return matches;
  } catch (error) {
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
          await File.findByIdAndUpdate(dbFile._id, { azurePath: best.path });
          syncOutputFileFromAzureToMongo(dbFile._id).catch((err) =>
            console.error("syncOutputFileFromAzureToMongo (find-files):", err.message)
          );
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

/** Max size to store raw XLSX in MongoDB (BSON doc limit is 16MB; leave headroom) */
const MAX_MONGO_FILE_BYTES = 12 * 1024 * 1024;

// Parse Lillybelle Excel and return data for the 4 Analysis charts
// Based on TogoInsight reference: sheets = "Location_Operator", KPI values in fixed cells
function parseLillybelleForCharts(buffer) {
  const kpiNames = ["SV1","SV2","SV3","SV4","NW1_3G","NW2_3G","TD1_3G","TD2_3G","TD3_3G","TD4_3G","NW1_4G","NW2_4G","TD1_4G","TD2_4G","TD3_4G","TD4_4G"];
  const kpiCells = ["P3","P4","P5","P6","P12","P13","P27","P35","P28","P36","P19","P20","P43","P51","P44","P52"];
  const colorLabels = ["Vert", "Jaune", "Orange", "Rouge"];
  const colorKeys = ["vert", "jaune", "orange", "rouge"];
  const testTypes = ["Voix", "Données 3G", "Données 4G"];
  const type2Kpi = {
    "Voix": { SV1:1, SV2:1, SV3:1, SV4:1 },
    "Données 3G": { NW1_3G:1, NW2_3G:1, TD1_3G:1, TD2_3G:1, TD3_3G:1, TD4_3G:1 },
    "Données 4G": { NW1_4G:1, NW2_4G:1, TD1_4G:1, TD2_4G:1, TD3_4G:1, TD4_4G:1 }
  };

  function kpi2Res(kpi, val) {
    val = parseFloat(String(val).replace(/%/g, "").replace(/,/g, ".").trim());
    if (isNaN(val)) return null;
    switch (kpi) {
      case "SV1": case "SV2":
        return val >= 98 ? "vert" : val >= 90 ? "jaune" : val >= 50 ? "orange" : "rouge";
      case "SV3":
        return val >= 3.5 ? "vert" : val >= 3.3 ? "jaune" : val >= 2.5 ? "orange" : "rouge";
      case "SV4":
        return val <= 1 ? "vert" : val <= 1.2 ? "jaune" : val <= 2 ? "orange" : "rouge";
      case "NW1_3G": case "NW1_4G":
        return val <= 1 ? "vert" : val <= 1.2 ? "jaune" : val <= 2 ? "orange" : "rouge";
      case "NW2_3G": case "NW2_4G":
        return val <= 5 ? "vert" : val <= 5.5 ? "jaune" : val <= 7.5 ? "orange" : "rouge";
      case "TD1_3G":
        return val >= 2 ? "vert" : val >= 1.8 ? "jaune" : val >= 1 ? "orange" : "rouge";
      case "TD2_3G":
        return val >= 3 ? "vert" : val >= 2.7 ? "jaune" : val >= 1.5 ? "orange" : "rouge";
      case "TD3_3G": case "TD4_3G":
        return val >= 96 ? "vert" : val >= 90 ? "jaune" : val >= 50 ? "orange" : "rouge";
      case "TD1_4G":
        return val >= 12 ? "vert" : val >= 10.8 ? "jaune" : val >= 6 ? "orange" : "rouge";
      case "TD2_4G":
        return val >= 25 ? "vert" : val >= 22.5 ? "jaune" : val >= 12.5 ? "orange" : "rouge";
      case "TD3_4G": case "TD4_4G":
        return val >= 99 ? "vert" : val >= 90 ? "jaune" : val >= 50 ? "orange" : "rouge";
      default: return null;
    }
  }

  function emptyRes() { return { vert: 0, jaune: 0, orange: 0, rouge: 0 }; }

  function countResults(opData, locFilter, kpiFilter) {
    const res = emptyRes();
    for (const loc of Object.keys(opData)) {
      if (locFilter && !(loc in locFilter)) continue;
      for (const kpi of Object.keys(opData[loc])) {
        if (kpiFilter && !(kpi in kpiFilter)) continue;
        const color = opData[loc][kpi];
        if (color in res) res[color]++;
      }
    }
    const total = res.vert + res.jaune + res.orange + res.rouge;
    res.okPct = total > 0 ? Math.round(10000 * res.vert / total) / 100 : 0;
    return res;
  }

  const data = {};
  try {
    const wb = XLSX.read(buffer, { type: "buffer" });
    wb.SheetNames.forEach(sheetName => {
      const parts = sheetName.split("_");
      if (parts.length < 2) return;
      const loc = parts[0];
      const op = parts.slice(1).join("_");
      if (!(op in data)) data[op] = {};
      data[op][loc] = {};
      const ws = wb.Sheets[sheetName];
      for (let i = 0; i < kpiNames.length; i++) {
        const cell = ws[kpiCells[i]];
        if (cell && cell.v != null) {
          const res = kpi2Res(kpiNames[i], cell.v);
          if (res) data[op][loc][kpiNames[i]] = res;
        }
      }
    });
  } catch (e) {
  }

  const opKeys = Object.keys(data);
  if (opKeys.length === 0) {
    return {
      chart1: { labels: colorLabels, togocel: [0, 0, 0, 0], moov: [0, 0, 0, 0] },
      chart2: { labels: [], togocel: [], moov: [] },
      chart3: { labels: kpiNames, togocel: kpiNames.map(() => 0), moov: kpiNames.map(() => 0) },
      chart4: { labels: testTypes, togocel: [0, 0, 0], moov: [0, 0, 0] }
    };
  }

  const togocelKey = opKeys.find(k => k.toLowerCase().includes("togo")) || opKeys[0];
  const moovKey = opKeys.find(k => k.toLowerCase().includes("moov")) || opKeys[1] || opKeys[0];

  const allLocs = {};
  opKeys.forEach(op => Object.keys(data[op]).forEach(loc => { allLocs[loc] = 1; }));
  const locLabels = Object.keys(allLocs);

  const c1togo = countResults(data[togocelKey]);
  const c1moov = countResults(data[moovKey]);

  const c2togo = [], c2moov = [];
  locLabels.forEach(loc => {
    const f = { [loc]: 1 };
    c2togo.push(countResults(data[togocelKey], f).okPct);
    c2moov.push(countResults(data[moovKey], f).okPct);
  });

  const c3togo = [], c3moov = [];
  kpiNames.forEach(kpi => {
    const f = { [kpi]: 1 };
    c3togo.push(countResults(data[togocelKey], undefined, f).okPct);
    c3moov.push(countResults(data[moovKey], undefined, f).okPct);
  });

  const c4togo = [], c4moov = [];
  testTypes.forEach(type => {
    c4togo.push(countResults(data[togocelKey], undefined, type2Kpi[type]).okPct);
    c4moov.push(countResults(data[moovKey], undefined, type2Kpi[type]).okPct);
  });

  return {
    chart1: { labels: colorLabels, togocel: colorKeys.map(c => c1togo[c]), moov: colorKeys.map(c => c1moov[c]) },
    chart2: { labels: locLabels, togocel: c2togo, moov: c2moov },
    chart3: { labels: kpiNames, togocel: c3togo, moov: c3moov },
    chart4: { labels: testTypes, togocel: c4togo, moov: c4moov }
  };
}

/**
 * After an output file is known in Azure, download it once and persist chart JSON (+ optional binary) in MongoDB.
 * Analysis tab reads chart data from Mongo when present.
 */
async function syncOutputFileFromAzureToMongo(fileId) {
  try {
    const fileDoc = await File.findById(fileId);
    if (!fileDoc || !["lillybelle", "arcep"].includes(fileDoc.fileType)) return;

    let filePath = fileDoc.azurePath || `OUTPUT/${fileDoc.fileName}`;
    const shouldResolve = !fileDoc.azurePath || fileDoc.azurePath.toLowerCase().includes(".csv.xlsx");
    if (shouldResolve) {
      const azureFiles = await findFilesInAzureByReference(fileDoc.fileReference);
      const match = pickBestAzureMatch(azureFiles, fileDoc.fileType);
      if (match) {
        filePath = match.path;
        await File.findByIdAndUpdate(fileDoc._id, { azurePath: match.path });
      }
    }

    const blockBlobClient = containerClient.getBlockBlobClient(filePath);
    if (!(await blockBlobClient.exists())) return;

    const downloadResponse = await blockBlobClient.download(0);
    const buffer = await streamToBuffer(downloadResponse.readableStreamBody);

    // Only set mongoFileStoredAt when mongoFileBinary is actually persisted (same rule as /api/lillybelle-chart-data)
    const update = {};
    if (buffer.length <= MAX_MONGO_FILE_BYTES) {
      update.mongoFileBinary = buffer;
      update.mongoFileStoredAt = new Date();
    }
    if (fileDoc.fileType === "lillybelle") {
      update.analysisChartData = parseLillybelleForCharts(buffer);
      update.analysisChartDataAt = new Date();
    }

    if (Object.keys(update).length === 0) return;

    await File.findByIdAndUpdate(fileId, { $set: update });
  } catch (e) {
    console.error("syncOutputFileFromAzureToMongo:", e.message);
  }
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

    // Prefer chart data persisted in MongoDB (synced from Azure when outputs were detected)
    if (
      fileRecord.analysisChartData &&
      typeof fileRecord.analysisChartData === "object" &&
      fileRecord.analysisChartData.chart1
    ) {
      return res.json({
        success: true,
        chartData: fileRecord.analysisChartData,
        source: "mongo"
      });
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

    const persist = {
      analysisChartData: chartData,
      analysisChartDataAt: new Date()
    };
    if (buffer.length <= MAX_MONGO_FILE_BYTES) {
      persist.mongoFileBinary = buffer;
      persist.mongoFileStoredAt = new Date();
    }
    await File.findByIdAndUpdate(fileRecord._id, { $set: persist });

    res.json({ success: true, chartData, source: "azure" });
  } catch (error) {
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
    res.status(500).json({ success: false, message: "Error deleting file" });
  }
});

const PORT = process.env.PORT || 3000;
let server;

mongoose.connection.once("open", () => {
  server = app.listen(PORT, () => {});
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