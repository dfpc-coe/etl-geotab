import fs from 'fs';
import ETL from '@tak-ps/etl';

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

        const fc = {
            type: 'FeatureCollection',
            features: []
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
