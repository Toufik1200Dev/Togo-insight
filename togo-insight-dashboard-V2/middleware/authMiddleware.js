const jwt = require("jsonwebtoken");
const User = require("../models/userModel");

const secret = process.env.JWT_SECRET || "e0994fb02524c80f839de457da95697811aa51dea6ed56f49b656e66094fb8c302517248cebcf024162beeb90bbdaebe75882ec7dd5d29bf689b750a8b8aa77f";

module.exports = async (req, res, next) => {
  try {
    const token = req.cookies?.token || 
                 (req.headers.authorization && req.headers.authorization.split(" ")[1]);

    if (!token) {
      return res.redirect('/login');
    }

    const decoded = jwt.verify(token, secret);
    
    // Get full user data from database
    const user = await User.findById(decoded._id).select('-password');
    if (!user) {
      return res.redirect('/login');
    }

    // Attach user data to request
    req.user = user;
    next();
  } catch (err) {
    console.error("Auth middleware error:", err);
    return res.redirect('/login');
  }
};
