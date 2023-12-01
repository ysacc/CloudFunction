const http = require('follow-redirects').http;
const https = require('follow-redirects').https;

const getAccessToken = async () => {
    const options = {
        'method': 'GET',
        'hostname': 'metadata.google.internal',
        'path': '/computeMetadata/v1/instance/service-accounts/default/token',
        'headers': {
            'Metadata-Flavor': 'Google'
        },
        'maxRedirects': 20
    };

    const result = new Promise(resolve => {
        const req = http.request(options, res => {
            const chunks = [];

            res.on("data", chunk => {
                chunks.push(chunk);
            });

            res.on("end", chunk => {
                const body = Buffer.concat(chunks);
                resolve(JSON.parse(body));
            });

            res.on("error", error => {
                console.error(error);
            });
        });
        req.end();
    });
    
    const r = await result;
    return r;
};

const getFileNameFromPath = (filepath) => {
    const lastSlashIndex = filepath.lastIndexOf('/');
    if (lastSlashIndex !== -1) {
        return filepath.substring(lastSlashIndex + 1);
    } else {
        return filepath; 
    }
};

const getTableName = (filepath) => {
    const filename = getFileNameFromPath(filepath);

    if (filename.startsWith('InTransitReception')) {
        return `InTransitReception`;
    } else if (filename.startsWith('PMMB2BReceptions')) {
        return `PMMB2BReceptions`;
    } else if (filename.startsWith('WmsReceptionsLoad')) {
        return `WmsReceptionsLoad`;
    } else if (filename.startsWith('WmsShipmentsLoad')) {
        return `WmsShipmentsLoad`;
    } else if (filename.startsWith('PMMB2BReturns')) {
        return `PMMB2BReturns`;
    } else if (filename.startsWith('PMMB2BSettlement')) {
        return `PMMB2BSettlement`;
    } else if (filename.startsWith('VtaIntercompany')) {
        return `VtaIntercompany`;
    } else if (filename.startsWith('Vta_Inter_CT2')) {
        return `Vta_Inter_CT2`;
    } else if (filename.startsWith('InTransitMain')) {
        return `InTransitMain`;
    } else if (filename.startsWith('WmsReceptionsLoad')) {
        return `WmsReceptionsLoad`;
    } else if (filename.startsWith('WmsShipmentsLoad')) {
        return `WmsShipmentsLoad`;
    } else {
        return `Error_InvalidFileName`;
    }
};

const loadFile = async (access_token, filepath) => {
    const tableName = getTableName(filepath);

    const options = {
        'method': 'POST',
        'hostname': 'sqladmin.googleapis.com',
        'path': '/v1/projects/spsa-test-on/instances/monitoreo/import',
        'headers': {
            'Authorization': `Bearer ${access_token}`,
            'Content-Type': 'application/json charset=utf-8'
        },
        'maxRedirects': 20
    };

    const result = new Promise(resolve => {
        const req = https.request(options, res => {
            const chunks = [];

            res.on("data", chunk => {
                chunks.push(chunk);
            });

            res.on("end", chunk => {
                const body = Buffer.concat(chunks);
                const response = JSON.parse(body);

                if (response.error && response.error.code === 404 && response.error.message.includes("Table not found")) {
                    console.error(`La tabla ${tableName} no existe.`);
                    resolve({ error: "Table not found" });
                } else {
                    // Log la cantidad de datos ingresados o filas afectadas
                    if (response.importedData && response.importedData.csvRows) {
                        console.log(`Se importaron ${response.importedData.csvRows} filas en la tabla ${tableName}.`);
                    }
                    
                    resolve(response);
                }
            });

            res.on("error", error => {
                console.error(`Error en loadFile: ${error}`);
                resolve({ error });
            });
        });

        const postData = {
            "importContext": {
                "fileType": "CSV",
                "uri": `gs://monitoreo-bucket-test/${filepath}`,
                "database": "DBMONITOREO",
                "csvImportOptions": {
                    "table": tableName
                }
            }
        };

        req.write(JSON.stringify(postData));

        req.end();

    });

    const r = await result;
    return r;
};

module.exports = {
    getAccessToken,
    loadFile,
};
