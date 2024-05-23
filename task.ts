import ETL, { Event, SchemaType, handler as internal, local, env } from '@tak-ps/etl';
import { Type, TSchema, Static } from '@sinclair/typebox';
import { FeatureCollection, Feature } from 'geojson';
import moment from 'moment-timezone'

const SchemaInput = Type.Object({
    'GEOTAB_USERNAME': Type.String({ description: 'GeoTab Username' }),
    'GEOTAB_PASSWORD': Type.String({ description: 'GeoTab Password' }),
    'GEOTAB_DATABASE': Type.String({ description: 'GeoTab Database - Usually OK to leave this blank', default: '' }),
    'GEOTAB_API': Type.String({ description: 'GeoTab API Endpoint', default: 'https://gov.geotabgov.us/' }),
    'GEOTAB_GROUPS': Type.Array(Type.Object({
        GroupId: Type.String({ description: 'The GeoTAB GroupID to Filter by' }),
    })),
    'DEBUG': Type.Boolean({ description: 'Print GeoJSON Features in logs', default: false })
});

export default class Task extends ETL {
    async schema(type: SchemaType = SchemaType.Input): Promise<TSchema> {
        if (type === SchemaType.Input) {
            return SchemaInput
        } else {
            return Type.Object({
                vin: Type.String(),
                licenseState: Type.String(),
                licensePlate: Type.String(),
                groups: Type.Array(Type.String())
            });
        }
    }

    async control() {
        const layer = await this.fetchLayer();

        if (!layer.environment.GEOTAB_USERNAME) throw new Error('No GEOTAB_USERNAME Provided');
        if (!layer.environment.GEOTAB_PASSWORD) throw new Error('No GEOTAB_PASSWORD Provided');
        if (!layer.environment.GEOTAB_DATABASE) layer.environment.GEOTAB_DATABASE = '';
        if (!layer.environment.GEOTAB_API) layer.environment.GEOTAB_API = 'https://gov.geotabgov.us';

        const env: Static<typeof SchemaInput> = layer.environment as Static<typeof SchemaInput>;

        const auth = await fetch(new URL(env.GEOTAB_API + '/apiv1'), {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                method: 'Authenticate',
                params: {
                    database: "",
                    userName: env.GEOTAB_USERNAME,
                    password: env.GEOTAB_PASSWORD
                }
            })
        });

        const credentials = (await auth.json()).result.credentials;

        const devices = await (await fetch(new URL(env.GEOTAB_API + '/apiv1'), {
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

        const info = await (await fetch(new URL(env.GEOTAB_API + '/apiv1'), {
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

        const filtered = {
            stale: 0,
            vin: 0
        };
        const fc: FeatureCollection = {
            type: 'FeatureCollection',
            features: devices.result.filter((d: any) => {
                const pass = moment(d.dateTime).isAfter(moment().subtract(1, 'hour'));
                if (!pass) ++filtered.stale;
                return pass;
            }).map((d: any) => {
                let callsign = d.device.id;
                const metadata: Record<string, string> = {};
                if (infoMap.has(d.device.id)) {
                    const info = infoMap.get(d.device.id);
                    if (!info.licenseState) info.licenseState = 'US';
                    callsign = info.licenseState + '-' + (info.licensePlate || 'Unknown')

                    metadata.vin = info.vehicleIdentificationNumber;
                    metadata.licenseState = info.licenseState;
                    metadata.licensePlate = info.licensePlate || 'Unknown';
                } else {
                    metadata.vin = 'UNKNOWN';
                    metadata.licenseState = 'UNKNOWN';
                    metadata.licensePlate = 'UNKNOWN';
                }

                metadata.groups = d.groups;

                const feat = {
                    id: `geotab-${d.device.id}`,
                    type: 'Feature',
                    properties: {
                        callsign,
                        course: d.bearing,
                        start: d.dateTime,
                        speed: d.speed * 0.277778, // Convert km/h => m/s
                        metadata
                    },
                    geometry: {
                        type: 'Point',
                        coordinates: [d.longitude, d.latitude]
                    }
                }

                return feat as Feature;
            }).filter((feat: Feature) => {
                if (env.GEOTAB_GROUPS && env.GEOTAB_GROUPS.length) {
                    for (const group of env.GEOTAB_GROUPS) {
                        if (feat.properties.metadata.groups.includes(group.GroupId)) return true;
                    }

                    return false;
                }

                return true;
            })
        };

        console.log(`ok - filtered ${filtered.stale} locations by staleness`);
        console.log(`ok - filtered ${filtered.vin} locations by vin`);

        await this.submit(fc);
    }
}

env(import.meta.url)
await local(new Task(), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(), event);
}

