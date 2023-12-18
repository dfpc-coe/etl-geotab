import fs from 'fs';
import ETL from '@tak-ps/etl';
import moment from 'moment-timezone'

try {
    const dotfile = new URL('.env', import.meta.url);

    fs.accessSync(dotfile);

    Object.assign(process.env, JSON.parse(fs.readFileSync(dotfile)));
    console.log('ok - .env file loaded');
} catch (err) {
    console.log('ok - no .env file loaded');
}

export default class Task extends ETL {
    static async schema(type = 'input') {
        if (type === 'input') {
            return {
                type: 'object',
                required: ['GEOTAB_USERNAME', 'GEOTAB_PASSWORD'],
                properties: {
                    'GEOTAB_USERNAME': {
                        type: 'string',
                        description: 'GeoTab Username'
                    },
                    'GEOTAB_PASSWORD': {
                        type: 'string',
                        description: 'GeoTab Password'
                    },
                    'GEOTAB_DATABASE': {
                        type: 'string',
                        description: 'GeoTab Database - Usually OK to leave this blank'
                    },
                    'GEOTAB_API': {
                        type: 'string',
                        description: 'GeoTab API Endpoint - Defaults to https://gov.geotabgov.us/'
                    },
                    'DEBUG': {
                        type: 'boolean',
                        default: false,
                        description: 'Print GeoJSON Features in logs'
                    }
                }
            };
        } else {
            return {
                type: 'object',
                required: [],
                properties: {
                }
            };
        }
    }

    async control() {
        const layer = await this.layer();

        if (!layer.environment.GEOTAB_USERNAME) throw new Error('No GEOTAB_USERNAME Provided');
        if (!layer.environment.GEOTAB_PASSWORD) throw new Error('No GEOTAB_PASSWORD Provided');
        if (!layer.environment.GEOTAB_DATABASE) layer.environment.GEOTAB_DATABASE = '';
        if (!layer.environment.GEOTAB_API) layer.environment.GEOTAB_API = 'https://gov.geotabgov.us';

        const auth = await fetch(new URL(layer.environment.GEOTAB_API + '/apiv1'), {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                method: 'Authenticate',
                params: {
                    database: "",
                    userName: layer.environment.GEOTAB_USERNAME,
                    password: layer.environment.GEOTAB_PASSWORD
                }
            })
        });

        const credentials = (await auth.json()).result.credentials;

        const devices = await (await fetch(new URL(layer.environment.GEOTAB_API + '/apiv1'), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                method: 'Get',
                params: {
                    credentials,
                    typeName: "DeviceStatusInfo",
                }
            })
        })).json();

        const info = await (await fetch(new URL(layer.environment.GEOTAB_API + '/apiv1'), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                method: 'Get',
                params: {
                    credentials,
                    typeName: "Device",
                }
            })
        })).json();

        const infoMap = new Map();
        for (const i of info.result) {
            infoMap.set(i.id, i);
        }

        const fc = {
            type: 'FeatureCollection',
            features: devices.result.filter((d) => {
                return moment(d.dateTime).isAfter(moment().subtract(1, 'hour'));
            }).map((d) => {
                let callsign = d.device.id;
                if (infoMap.has(d.device.id)) {
                    const info = infoMap.get(d.device.id);
                    if (!info.licenseState) info.licenseState = 'US';
                    callsign = info.licenseState + '-' + (info.licensePlate || 'Unknown')
                }

                const feat = {
                    id: `geotab-${d.device.id}`,
                    type: 'Feature',
                    properties: {
                        callsign,
                        course: d.bearing,
                        start: d.dateTime,
                        speed: d.speed * 0.277778 // Convert km/h => m/s
                    },
                    geometry: {
                        type: 'Point',
                        coordinates: [d.longitude, d.latitude]
                    }
                }

                return feat;
            })
        };

        await this.submit(fc);
    }
}

export async function handler(event = {}) {
    if (event.type === 'schema:input') {
        return await Task.schema('input');
    } else if (event.type === 'schema:output') {
        return await Task.schema('output');
    } else {
        const task = new Task();
        await task.control();
    }
}

if (import.meta.url === `file://${process.argv[1]}`) handler();
