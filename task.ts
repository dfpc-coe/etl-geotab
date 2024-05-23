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
    'GEOTAB_PREFIX': Type.String({ default: '', description: 'Filter by prefix of name fielt' }),
    'DEBUG': Type.Boolean({ description: 'Print GeoJSON Features in logs', default: false })
});

export default class Task extends ETL {
    async schema(type: SchemaType = SchemaType.Input): Promise<TSchema> {
        if (type === SchemaType.Input) {
            return SchemaInput
        } else {
            return Type.Object({
                vin: Type.String(),
                name: Type.String(),
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
        if (!layer.environment.GEOTAB_PREFIX) layer.environment.GEOTAB_PREFIX = '';
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
                    database: env.GEOTAB_DATABASE,
                    userName: env.GEOTAB_USERNAME,
                    password: env.GEOTAB_PASSWORD
                }
            })
        });

        const credentials = (await auth.json()).result.credentials;

        const info = await (await fetch(new URL(env.GEOTAB_API + '/apiv1'), {
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

        const params: any = {
            credentials,
            typeName: "Device",
            search: {
                excludeUntrackedAssets: true,
                fromDate: moment().subtract(1, 'hour').toISOString()
            }
        }

        if (env.GEOTAB_GROUPS && env.GEOTAB_GROUPS.length) {
            params.search.groups = env.GEOTAB_GROUPS.map((g) => {
                return { id: g.GroupId };
            });
        }

        const devices = await (await fetch(new URL(env.GEOTAB_API + '/apiv1'), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                method: 'Get',
                params: params
            })
        })).json();

        const infoMap = new Map();
        for (const i of info.result) {
            infoMap.set(i.device.id, i);
        }

        const fc: FeatureCollection = {
            type: 'FeatureCollection',
            features: devices.result.map((d: any) => {
                let callsign = d.id;
                const metadata: Record<string, string> = {};

                const info = infoMap.get(d.id);
                if (!info) return null;

                if (!d.licenseState) d.licenseState = 'US';
                callsign = d.licenseState + '-' + (d.licensePlate || 'Unknown')

                metadata.vin = d.vehicleIdentificationNumber;
                metadata.licenseState = d.licenseState;
                metadata.licensePlate = d.licensePlate || 'Unknown';
                metadata.groups = info.groups;
                metadata.name = d.name || 'No Name';

                const feat = {
                    id: `geotab-${info.device.id}`,
                    type: 'Feature',
                    properties: {
                        callsign,
                        course: info.bearing,
                        start: info.dateTime,
                        speed: info.speed * 0.277778, // Convert km/h => m/s
                        metadata
                    },
                    geometry: {
                        type: 'Point',
                        coordinates: [info.longitude, info.latitude]
                    }
                }

                return feat as Feature;
            }).filter((f: Feature | null) => {
                return f !== null;
            }).filter((f: Feature) => {
                return f.properties.metadata.name.startsWith(env.GEOTAB_PREFIX);
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

