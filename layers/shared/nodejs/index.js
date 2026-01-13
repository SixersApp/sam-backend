"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createHandler = exports.express = exports.createApp = exports.pg = exports.getPool = void 0;
// Type augmentations
require("./types");
// Re-export everything
var db_1 = require("./db");
Object.defineProperty(exports, "getPool", { enumerable: true, get: function () { return db_1.getPool; } });
Object.defineProperty(exports, "pg", { enumerable: true, get: function () { return db_1.pg; } });
var app_factory_1 = require("./app-factory");
Object.defineProperty(exports, "createApp", { enumerable: true, get: function () { return app_factory_1.createApp; } });
Object.defineProperty(exports, "express", { enumerable: true, get: function () { return app_factory_1.express; } });
var handler_1 = require("./handler");
Object.defineProperty(exports, "createHandler", { enumerable: true, get: function () { return handler_1.createHandler; } });
