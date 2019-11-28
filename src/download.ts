const fs = require("fs");
const NetCDFReader = require("netcdfjs");
import cluster from "cluster";
import os from "os";
import http from "http";
import { MongoClient, Db } from "mongodb";
import assert from "assert";


async function download() {
  if (cluster.isMaster) {
    setInterval(masterProcess, 1000*60*60*24)
    masterProcess();
  } else {
    childProcess();
  }
}
download();

function masterProcess() {
  
  let numCPUs = os.cpus().length;
  console.time("executiontime");
  // Database
  const dbUrl = process.env.url || "mongodb://mongodb:27017";
  const dbName = "fcoodb";
  const client = new MongoClient(dbUrl, {
    useNewUrlParser: true,
    useUnifiedTopology: true
  });
  // Data configs
  const dataUrl =
    "http://wms.fcoo.dk/webmap/FCOO/GETM/idk-600m_3D-velocities_surface_1h.DK600-v007C.nc";
  const dest = "data.nc";

  var download = async function(url: string, dest: string, cb: () => void) {
    let file = fs.createWriteStream(dest);
    var request = http.get(url, function(response) {
      response.pipe(file);
      file.on("finish", function() {
        file.close(cb);
      });
    });
    cb();
  };

  const clearOverlappingCollections = async (
    db: Db,
    reader: any,
    timestamp: number
  ) => {
    let newCollectionNames: string[] = [];
    for (const time of reader.getDataVariable("time")) {
      newCollectionNames.push(`${timestamp + time}`);
    }

    await db.collections().then(async collections => {
      for (const collection of collections) {
        db.dropCollection(collection.collectionName);
        console.log(collection.collectionName);
      }
    });
  };

  download(dataUrl, dest, () => {
    client.connect(async err => {
      console.log('started')
      assert.equal(null, err, 'something went wrong with the database connection');
      const db: Db = client.db(dbName);

      const data = fs.readFileSync("data.nc");
      const reader = new NetCDFReader(data); // read the header
      const lat = reader.getDataVariable("latc");
      const lon = reader.getDataVariable("lonc");
      let date: any = new Date(reader.variables[3].attributes[1].value);
      let timestamp = Math.floor(date / 1000);

      await clearOverlappingCollections(db, reader, timestamp);
      console.log('getting owned')
      let workers: cluster.Worker[] = [];
      let remainingWork = reader.getDataVariable("time");
      let index = 0;
      let done = remainingWork.length;
      let amountStored = 0;

      for (let i = 0; i < numCPUs; i++) {
        const worker = cluster.fork();
        workers.push(worker);

        worker.on("message", async message => {
          if (message.result) {
            let collection = db.collection(message.collectionName);
            collection.createIndex({ location: "2dsphere" });
            await collection.insertMany(message.result).then(() => {
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
              console.log(index)
            }
          }
        });
      }

      workers.forEach(function(worker) {
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
    });
  });
}

function childProcess() {
  const calculateMagnitude = (x: number, y: number) => {
    return Math.sqrt(Math.pow(x, 2) + Math.pow(y, 2));
  };

  const calculateDirection = (x: number, y: number) => {
    let degrees = (Math.atan(x / y) * 180) / Math.PI;
    if (x > 0 && y < 0) {
      degrees += 180;
    } else if (x < 0 && y < 0) {
      degrees = Math.abs(degrees) + 180;
    } else if (x < 0 && y > 0) {
      degrees += 360;
    } else {
      degrees = Math.abs(degrees);
    }
    return degrees;
  };

  const createCurrentData = (data: any): any[] => {
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

  process.on("message", async message => {
    if (message.cmd == "work") {
      let collectionName = `${message.timestamp + message.time}`;
      let result = createCurrentData(message.data);
      (<any>process).send({ result: result, collectionName: collectionName });
    }
  });
}
