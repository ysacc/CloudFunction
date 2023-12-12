const http = require('follow-redirects').http;
const https = require('follow-redirects').https;
const request = require('request');

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
    } else if (filename.startsWith('VentaIntercompany')) {
        return `VentaIntercompany`;
    } else if (filename.startsWith('Vta_Inter_CT2')) {
        return `Vta_Inter_CT2`;
    } else if (filename.startsWith('InTransitMain')) {
        return `InTransitMain`;
    } else if (filename.startsWith('WmsReceptionsLoad')) {
        return `WmsReceptionsLoad`;
    } else if (filename.startsWith('WmsShipmentsLoad')) {
        return `WmsShipmentsLoad`;
    } else if (filename.startsWith('WMS_LOAD')) {
        return `WMS_LOAD`;
    } else if (filename.startsWith('WMS_SHIPMENT')) {
        return `WMS_SHIPMENT`;
    } else {
        return `Error_InvalidFileName`;
    }
};

const loadFile = async (access_token, filepath) => {
    const tableName = getTableName(filepath);

    const options = {
        'method': 'POST',
        'hostname': 'sqladmin.googleapis.com',
        'path': '/v1/projects/spsa-prd-on/instances/monitoreo/import',
        'headers': {
            'Authorization': `Bearer ${access_token}`,
            'Content-Type': 'application/json charset=utf-8'
        },
        'maxRedirects': 20
    };

    const result = new Promise(resolve => {
        
        const postData = {
            "importContext": {
                "fileType": "CSV",
                "uri": `gs://monitoreo-bucket-prd/${filepath}`,
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

const updateData = (filepath) => {
    //update data
    let payloadRequest = {
        url: getUrlupdate(filepath),
        method: 'GET',
        headers: {
            "Content-Type": "application/json"
        },
        json: true
    };

    requestHttp(payloadRequest);
}

let requestHttp = async function (payload) {
    let promiseRequest = new Promise(function (resolve, reject) {        
        request(payload, function (error, response, body) {
            try {
                if (error) {
                    reject(error);
                } else {
                    if (body) {
                        resolve(body);
                    }
                    else {
                        resolve(response);
                    }
                }                
            } catch (err) {                
                reject(err)
            }
        });
    });
    return promiseRequest;
}

const getUrlupdate = (filepath) => {
    const filename = getFileNameFromPath(filepath);
    if (filename.startsWith('InTransitMain')) {
        return process.env.HOST_API + `/in-transit/update/InTransitMain`;
    } else if (filename.startsWith('InTransitReception')) {
        return process.env.HOST_API + `/in-transit/update/InTransitReception`;
    } else if (filename.startsWith('PMMB2BReceptions')) {
        return process.env.HOST_API + `/pmm-b2b/update/PMMB2BReceptions`;
    } else if (filename.startsWith('PMMB2BReturns')) {
        return process.env.HOST_API + `/pmm-b2b/update/PMMB2BReturns`;
    } else if (filename.startsWith('PMMB2BSettlement')) {
        return process.env.HOST_API + `/pmm-b2b/update/PMMB2BSettlement`;
    } else if (filename.startsWith('VentaIntercompany')) {
        return process.env.HOST_API + `/facturador/update/VentaIntercompany`;
    } else if (filename.startsWith('WmsReceptionsLoad')) {
        return process.env.HOST_API + `/wms-pmm/update/WmsReceptionsLoad`;    
    } else if (filename.startsWith('WmsShipmentsLoad')) {
        return process.env.HOST_API + `/wms-pmm/update/WmsShipmentsLoad`;
    } else {
        return `Error_InvalidFileName`;
    }
};

module.exports = {
    getAccessToken,
    loadFile,
    updateData
};
