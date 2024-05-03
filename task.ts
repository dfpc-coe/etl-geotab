import fs from 'fs';
import ETL, { Event, SchemaType, handler as internal, local, env } from '@tak-ps/etl';
import { Type, TSchema } from '@sinclair/typebox';
import { FeatureCollection, Feature, Geometry } from 'geojson';
import moment from 'moment-timezone'

export default class Task extends ETL {
    async schema(type: SchemaType = SchemaType.Input): Promise<TSchema> {
        if (type === SchemaType.Input) {
            return Type.Object({
                'GEOTAB_USERNAME': Type.String({ description: 'GeoTab Username' }),
                'GEOTAB_PASSWORD': Type.String({ description: 'GeoTab Password' }),
                'GEOTAB_DATABASE': Type.String({ description: 'GeoTab Database - Usually OK to leave this blank', default: '' }),
                'GEOTAB_API': Type.String({ description: 'GeoTab API Endpoint', default: 'https://gov.geotabgov.us/' }),
                'GEOTAB_FILTER': Type.Boolean({
                    description: 'Filter by GeoTAB entries that are sucessfully joined with the GEOTAB_AUGMENT data',
                    default: false
                }),
                'GEOTAB_AUGMENT': Type.Array(Type.Object({
                    vin: Type.String()
                })),
                'DEBUG': Type.Boolean({ description: 'Print GeoJSON Features in logs', default: false })
            });
        } else {
            return Type.Object({
                licenseState: Type.String(),
                licensePlate: Type.String()
            });
        }
    }

    async control() {
        const layer = await this.fetchLayer();

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

        const fc: FeatureCollection = {
            type: 'FeatureCollection',
            features: devices.result.filter((d: any) => {
                return moment(d.dateTime).isAfter(moment().subtract(1, 'hour'));
            }).map((d: any) => {
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
                        speed: d.speed * 0.277778, // Convert km/h => m/s
                        metadata: {
                            licenseState: info.licenseState,
                            licensePlate: info.licensePlate
                        }
                    },
                    geometry: {
                        type: 'Point',
                        coordinates: [d.longitude, d.latitude]
                    }
                }

                return feat as Feature;
            })
        };

        await this.submit(fc);
    }
}

env(import.meta.url)
await local(new Task(), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(), event);
}

