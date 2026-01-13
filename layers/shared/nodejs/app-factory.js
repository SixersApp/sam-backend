"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.express = exports.createApp = void 0;
const express_1 = __importDefault(require("express"));
exports.express = express_1.default;
const cors_1 = __importDefault(require("cors"));
const createApp = () => {
    const app = (0, express_1.default)();
    app.use((0, cors_1.default)());
    app.use(express_1.default.json());
    return app;
};
exports.createApp = createApp;
