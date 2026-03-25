const mongoose = require("mongoose");
const crypto = require("crypto");

const fileSchema = new mongoose.Schema({
    userId: { type: mongoose.Schema.Types.ObjectId, ref: "User", required: true },
    fileName: { type: String, required: true },
    originalName: { type: String, required: true },
    fileToken: { type: String, default: () => crypto.randomBytes(16).toString('hex') },
    uploadedAt: { type: Date, default: Date.now },
    fileReference: { type: String, default: () => Math.floor(Math.random() * 1000000).toString() },
    fileType: { type: String, enum: ['lillybelle', 'arcep', 'original'], default: 'original' },
    relatedFiles: [{ type: mongoose.Schema.Types.ObjectId, ref: "File" }],
    azurePath: { type: String },
    isReady: { type: Boolean, default: false },
    // Cached from Azure for Analysis tab + optional download fallback (not selected by default — use .select('+mongoFileBinary'))
    analysisChartData: { type: mongoose.Schema.Types.Mixed },
    analysisChartDataAt: { type: Date },
    mongoFileBinary: { type: Buffer, select: false },
    mongoFileStoredAt: { type: Date }
});

module.exports = mongoose.model("File", fileSchema);
