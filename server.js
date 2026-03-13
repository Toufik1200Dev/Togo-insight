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
      const decoded = jwt.verify(token, process.env.JWT_SECRET || "e0994fb02524c80f839de457da95697811aa51dea6ed56f49b656e66094fb8c302517248cebcf024162beeb90bbdaebe75882ec7dd5d29bf689b750a8b8aa77f");
      const user = await User.findById(decoded._id);
      req.user = user;
    }
  } catch (error) {
    console.error("Auth middleware error:", error);
  }
  next();
});

// Upload to Azure
app.post("/upload", authMiddleware, upload.single("file"), async (req, res) => {
  if (!req.file) return res.status(400).json({ message: "No file uploaded." });
  
  try {
    const originalFileName = req.file.originalname;
    const userId = req.user._id;
    
    // Extract reference number from the file name if possible
    // Assuming the file names follow pattern like "RawData_ExportToCsv_20250204200349.csv"
    let fileReference = '';
    const referenceMatch = originalFileName.match(/\d+/);
    if (referenceMatch) {
      fileReference = referenceMatch[0]; // Use the first sequence of numbers found
    } else {
      // Fallback to random number if no reference can be extracted
      fileReference = Math.floor(Math.random() * 1000000).toString();
    }
    
    console.log(`File Reference: ${fileReference}`);
    
    // 1. Upload original file to Azure INPUT folder
    const inputBlobClient = containerClient.getBlockBlobClient(`INPUT/${originalFileName}`);
    await inputBlobClient.uploadData(req.file.buffer, {
      blobHTTPHeaders: { blobContentType: req.file.mimetype }
    });
    
    console.log(`File uploaded to INPUT folder: ${originalFileName}`);
    
    // Create placeholders in database for expected output files
    // These will be updated when the files are detected in Azure storage
    
    // Lillybelle file record (expected)
    const lillybelleFileName = `lillybelle_output_${fileReference}.xlsx`;
    const lillybelleFile = new File({
      userId,
      fileName: lillybelleFileName,
      originalName: originalFileName,
      fileReference: fileReference,
      fileType: 'lillybelle'
    });
    await lillybelleFile.save();
    
    // ARCEP file record (expected)
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

// List all files in a specified Azure container/folder
app.get("/azure-files/:container/:prefix?", authMiddleware, async (req, res) => {
  try {
    const containerName = req.params.container;
    const prefix = req.params.prefix || "";
    
    if (containerName !== "INPUT" && containerName !== "OUTPUT") {
      return res.status(400).json({ 
        success: false, 
        message: "Invalid container name. Must be INPUT or OUTPUT."
      });
    }
    
    const files = [];
    const options = { prefix: prefix };
    
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
    
    // If azurePath is not set, try to find matching files in Azure
    if (!fileRecord.azurePath) {
      console.log(`No azure path set, searching for matching files with reference: ${fileRecord.fileReference}`);
      
      // Find actual files in Azure by reference
      const azureFiles = await findFilesInAzureByReference(fileRecord.fileReference);
      
      if (azureFiles.length > 0) {
        // Look for a match for this specific file
        let matchedFile = null;
        
        for (const azureFile of azureFiles) {
          if (azureFile.name.toLowerCase().includes(fileName.toLowerCase()) ||
              (azureFile.name.toLowerCase().includes('lillybelle') && fileRecord.fileType === 'lillybelle') ||
              (azureFile.name.toLowerCase().includes('arcep') && fileRecord.fileType === 'arcep')) {
            matchedFile = azureFile;
            break;
          }
        }
        
        if (matchedFile) {
          console.log(`Found matching file in Azure: ${matchedFile.path}`);
          filePath = matchedFile.path;
          
          // Update the database record with actual path
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
      
      // Set correct content type based on file extension
      const contentType = filePath.toLowerCase().endsWith('.xlsx') 
        ? 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        : 'text/csv';
      
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

// Find files by reference in Azure storage
app.get("/find-files-by-reference/:reference", authMiddleware, async (req, res) => {
  try {
    const reference = req.params.reference;
    
    // Find database records for this reference
    const dbFiles = await File.find({ fileReference: reference });
    
    // Find actual files in Azure
    const azureFiles = await findFilesInAzureByReference(reference);
    
    // Update database if files are found
    if (azureFiles.length > 0 && dbFiles.length > 0) {
      for (const azureFile of azureFiles) {
        // Find if this matches any expected DB file
        for (const dbFile of dbFiles) {
          // Update if this file matches an expected pattern
          if (azureFile.name.toLowerCase().includes(dbFile.fileName.toLowerCase()) ||
              (azureFile.name.toLowerCase().includes('lillybelle') && dbFile.fileType === 'lillybelle') ||
              (azureFile.name.toLowerCase().includes('arcep') && dbFile.fileType === 'arcep')) {
            
            console.log(`Updating file record ${dbFile._id} with actual Azure path: ${azureFile.path}`);
            
            // Update the database record with the actual file path
            await File.findByIdAndUpdate(dbFile._id, {
              azurePath: azureFile.path
            });
          }
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

// Parse Lillybelle Excel and return data for the 4 Analysis charts
function parseLillybelleForCharts(buffer) {
  const result = {
    chart1: { labels: ["Vert", "Jaune", "Orange", "Rouge"], togocel: [45, 0, 0, 3.5], moov: [29, 4, 4, 11] },
    chart2: { labels: ["L1", "L2", "L3"], togocel: [88, 94, 98], moov: [69, 5, 75] },
    chart3: {
      labels: ["SV1", "SV2", "SV3", "SV4", "NW1_3G", "NW2_3G", "TD1_3G", "TD2_3G", "TD3_3G", "TD4_3G", "NW1_4G", "NW2_4G", "TD1_4G", "TD2_4G", "TD3_4G", "TD4_4G"],
      togocel: [100, 100, 100, 0, 95, 95, 90, 60, 5, 20, 95, 95, 85, 90, 95, 100],
      moov: [85, 88, 90, 0, 20, 60, 90, 85, 95, 90, 25, 28, 85, 90, 100, 100]
    },
    chart4: { labels: ["Voix", "Données 3G", "Données 4G"], togocel: [100, 90, 95], moov: [50, 67, 72] }
  };
  try {
    const wb = XLSX.read(buffer, { type: "buffer", cellDates: false });
    const firstSheetName = wb.SheetNames[0];
    const ws = wb.Sheets[firstSheetName];
    const rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });
    if (!rows || rows.length < 2) return result;
    const header = rows[0].map(h => (h != null ? String(h).trim() : ""));
    const colIdx = (name) => header.findIndex(h => h && h.toUpperCase().includes(name));
    const ensembleIdx = colIdx("ENSEMBLE") >= 0 ? colIdx("ENSEMBLE") : header.length - 1;
    const pointIndices = [1, 2, 3].map(i => header.findIndex(h => h && (h === "POINT" + i || h === "POINT" + (i < 10 ? "0" + i : i)))).filter(i => i >= 0);
    const num = (v) => (v == null || v === "" ? NaN : typeof v === "number" ? v : parseFloat(String(v).replace(",", ".")));
    for (let r = 1; r < rows.length; r++) {
      const row = rows[r];
      const calc = row[0] != null ? String(row[0]).trim() : "";
      const val = num(row[ensembleIdx]);
      if (isNaN(val)) continue;
      if (calc.includes("SV1") || calc.includes("SetupTime")) result.chart3.togocel[0] = Math.min(120, Math.round(val));
      if (calc.includes("SV2")) result.chart3.togocel[1] = Math.min(120, Math.round(val));
      if (calc.includes("SV3") || calc.includes("MOS")) result.chart3.togocel[2] = Math.min(120, Math.round(val));
      if (calc.includes("SV4")) result.chart3.togocel[3] = Math.min(120, Math.round(val));
      if (calc.includes("NW1") && calc.includes("3G")) result.chart3.togocel[4] = Math.min(120, Math.round(val));
      if (calc.includes("NW2") && calc.includes("3G")) result.chart3.togocel[5] = Math.min(120, Math.round(val));
      if (pointIndices.length >= 3 && (calc.includes("SV") || calc.includes("Setup"))) {
        result.chart2.togocel[0] = Math.min(120, Math.round(num(row[pointIndices[0]]) || result.chart2.togocel[0]));
        result.chart2.togocel[1] = Math.min(120, Math.round(num(row[pointIndices[1]]) || result.chart2.togocel[1]));
        result.chart2.togocel[2] = Math.min(120, Math.round(num(row[pointIndices[2]]) || result.chart2.togocel[2]));
      }
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
    if (!fileRecord.azurePath) {
      const azureFiles = await findFilesInAzureByReference(reference);
      const match = azureFiles.find(af => af.name.toLowerCase().includes("lillybelle"));
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