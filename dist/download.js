"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const fs = require("fs");
const NetCDFReader = require("netcdfjs");
const cluster_1 = __importDefault(require("cluster"));
const os_1 = __importDefault(require("os"));
const http_1 = __importDefault(require("http"));
const mongodb_1 = require("mongodb");
const assert_1 = __importDefault(require("assert"));
function download() {
    return __awaiter(this, void 0, void 0, function* () {
        if (cluster_1.default.isMaster) {
            setInterval(masterProcess, 1000 * 60 * 60 * 24);
            masterProcess();
        }
        else {
            childProcess();
        }
    });
}
download();
function masterProcess() {
    let numCPUs = os_1.default.cpus().length;
    console.time("executiontime");
    // Database
    const dbUrl = process.env.url || "mongodb://mongodb:27017";
    const dbName = "fcoodb";
    const client = new mongodb_1.MongoClient(dbUrl, {
        useNewUrlParser: true,
        useUnifiedTopology: true
    });
    // Data configs
    const dataUrl = "http://wms.fcoo.dk/webmap/FCOO/GETM/idk-600m_3D-velocities_surface_1h.DK600-v007C.nc";
    const dest = "data.nc";
    var download = function (url, dest, cb) {
        return __awaiter(this, void 0, void 0, function* () {
            let file = fs.createWriteStream(dest);
            var request = http_1.default.get(url, function (response) {
                response.pipe(file);
                file.on("finish", function () {
                    file.close(cb);
                });
            });
            cb();
        });
    };
    const clearOverlappingCollections = (db, reader, timestamp) => __awaiter(this, void 0, void 0, function* () {
        let newCollectionNames = [];
        for (const time of reader.getDataVariable("time")) {
            newCollectionNames.push(`${timestamp + time}`);
        }
        yield db.collections().then((collections) => __awaiter(this, void 0, void 0, function* () {
            for (const collection of collections) {
                db.dropCollection(collection.collectionName);
                console.log(collection.collectionName);
            }
        }));
    });
    download(dataUrl, dest, () => {
        client.connect((err) => __awaiter(this, void 0, void 0, function* () {
            console.log('started');
            assert_1.default.equal(null, err, 'something went wrong with the database connection');
            const db = client.db(dbName);
            const data = fs.readFileSync("data.nc");
            const reader = new NetCDFReader(data); // read the header
            const lat = reader.getDataVariable("latc");
            const lon = reader.getDataVariable("lonc");
            let date = new Date(reader.variables[3].attributes[1].value);
            let timestamp = Math.floor(date / 1000);
            yield clearOverlappingCollections(db, reader, timestamp);
            console.log('getting owned');
            let workers = [];
            let remainingWork = reader.getDataVariable("time");
            let index = 0;
            let done = remainingWork.length;
            let amountStored = 0;
            for (let i = 0; i < numCPUs; i++) {
                const worker = cluster_1.default.fork();
                workers.push(worker);
                worker.on("message", (message) => __awaiter(this, void 0, void 0, function* () {
                    if (message.result) {
                        let collection = db.collection(message.collectionName);
                        collection.createIndex({ location: "2dsphere" });
                        yield collection.insertMany(message.result).then(() => {
                            amountStored++;
                        });
                        if (amountStored >= done) {
                            console.timeEnd("executiontime");
                        }
                        if (index < done) {
                            let time = remainingWork[index];
                            let dataChunkUU = reader.getDataVariable("uu")[time / 3600];
                            let dataChunkVV = reader.getDataVariable("vv")[time / 3600];
                            worker.send({
                                cmd: "work",
                                timestamp: timestamp,
                                time: remainingWork[index],
                                data: { uu: dataChunkUU, vv: dataChunkVV, latc: lat, lonc: lon }
                            });
                            index++;
                            console.log(index);
                        }
                    }
                }));
            }
            workers.forEach(function (worker) {
                if (index < done) {
                    let time = remainingWork[index];
                    let dataChunkUU = reader.getDataVariable("uu")[time / 3600];
                    let dataChunkVV = reader.getDataVariable("vv")[time / 3600];
                    worker.send({
                        cmd: "work",
                        timestamp: timestamp,
                        time: remainingWork[index],
                        data: { uu: dataChunkUU, vv: dataChunkVV, latc: lat, lonc: lon }
                    });
                    index++;
                }
            });
        }));
    });
}
function childProcess() {
    const calculateMagnitude = (x, y) => {
        return Math.sqrt(Math.pow(x, 2) + Math.pow(y, 2));
    };
    const calculateDirection = (x, y) => {
        let degrees = (Math.atan(x / y) * 180) / Math.PI;
        if (x > 0 && y < 0) {
            degrees += 180;
        }
        else if (x < 0 && y < 0) {
            degrees = Math.abs(degrees) + 180;
        }
        else if (x < 0 && y > 0) {
            degrees += 360;
        }
        else {
            degrees = Math.abs(degrees);
        }
        return degrees;
    };
    const createCurrentData = (data) => {
        let documents = [];
        let index = 0;
        for (let lat = 0; lat < data.latc.length; lat++) {
            for (let lon = 0; lon < data.lonc.length; lon++) {
                let xVel = data.uu[index];
                let yVel = data.vv[index];
                if (xVel > -1000 && yVel > -1000) {
                    let mag = calculateMagnitude(xVel, yVel);
                    let dir = calculateDirection(xVel, yVel);
                    documents.push({
                        xVelocity: xVel,
                        yVelocity: yVel,
                        magnitude: mag,
                        direction: dir,
                        location: {
                            type: "Point",
                            coordinates: [data.latc[lat], data.lonc[lon]]
                        }
                    });
                }
                index++;
            }
        }
        return documents;
    };
    process.on("message", (message) => __awaiter(this, void 0, void 0, function* () {
        if (message.cmd == "work") {
            let collectionName = `${message.timestamp + message.time}`;
            let result = createCurrentData(message.data);
            process.send({ result: result, collectionName: collectionName });
        }
    }));
}
//# sourceMappingURL=download.js.map