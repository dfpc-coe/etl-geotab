import ETL, { Event, SchemaType, handler as internal, local, env, fetch } from '@tak-ps/etl';
import { Type, TSchema, Static } from '@sinclair/typebox';
import { FeatureCollection, Feature } from 'geojson';
import moment from 'moment-timezone'

const Credentials = Type.Object({
    database: Type.String(),
    sessionId: Type.String(),
    userName: Type.String()
})

const GEOTAB_DeviceInfo = Type.Object({
    bearing: Type.Number(),
    currentStateDuration: Type.String(),
    exceptionEvents: Type.Array(Type.Unknown()),
    isDeviceCommunicating: Type.Boolean(),
    isDriving: Type.Boolean(),
    latitude: Type.Number(),
    longitude: Type.Number(),
    speed: Type.Number(),
    dateTime: Type.String(),
    device: Type.Object({
        id: Type.String()
    }),
    driver: Type.Unknown(),
    isHistoricLastDriver: Type.Boolean(),
    groups: Type.Array(Type.Object({
        id: Type.String()
    }))
});

export const Result = Type.Object({
    info: Type.Optional(Type.Array(GEOTAB_DeviceInfo)),

    // TODO Type these
    devices: Type.Optional(Type.Array(Type.Any())),
    drivers: Type.Optional(Type.Array(Type.Any()))
});

const SchemaInput = Type.Object({
    'GEOTAB_USERNAME': Type.String({ description: 'GeoTab Username' }),
    'GEOTAB_PASSWORD': Type.String({ description: 'GeoTab Password' }),
    'GEOTAB_DATABASE': Type.String({ description: 'GeoTab Database - Usually OK to leave this blank', default: '' }),
    'GEOTAB_API': Type.String({
        description: 'GeoTab API Endpoint',
        default: 'https://gov.geotabgov.us/'
    }),
    'GEOTAB_GROUPS': Type.Array(Type.Object({
        GroupId: Type.String({ description: 'The GeoTAB GroupID to Filter by' }),
    })),
    'GEOTAB_PREFIX': Type.String({ default: '', description: 'Filter by prefix of name field' }),
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
                driver: Type.String(),
                licenseState: Type.String(),
                licensePlate: Type.String(),
                groups: Type.Array(Type.String())
            });
        }
    }

    async control() {
        const layer = await this.fetchLayer();
        const env = await this.env(SchemaInput);

        let credentials: Static<typeof Credentials>;
        if (Object.keys(layer.ephemeral).length) {
            try {
                credentials = layer.ephemeral as Static<typeof Credentials>;
                await this.user(env, credentials);
                console.log('ok - using cached credentials');
            } catch (err) {
                console.warn('Failed user check, reauthenticating', err);
                credentials = await this.login(env)
                this.setEphemeral(credentials);
            }
        } else  {
            credentials = await this.login(env)
            this.setEphemeral(credentials);
        }

        const res: Static<typeof Result> = {};

        await Promise.all([
            (async () => {
                const req = await fetch(new URL(env.GEOTAB_API + '/apiv1'), {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        method: 'Get',
                        params: {
                            credentials,
                            typeName: "DeviceStatusInfo",
                        }
                    })
                });

                const body = await req.typed(Type.Object({
                    result: Type.Array(GEOTAB_DeviceInfo)
                }))

                res.info = body.result;
            })(),
            (async () => {
                const req = await fetch(new URL(env.GEOTAB_API + '/apiv1'), {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        method: 'Get',
                        params: {
                            credentials,
                            search: {
                                isDriver: true
                            },
                            typeName: 'User',
                        }
                    })
                });

                const body = await req.typed(Type.Object({
                    result: Type.Array(Type.Any())
                }));

                res.drivers = body.result;
            })(),
            (async () => {
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

                const req = await fetch(new URL(env.GEOTAB_API + '/apiv1'), {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        method: 'Get',
                        params: params
                    })
                });

                const body  = await req.typed(Type.Object({
                    result: Type.Array(Type.Any())
                }));

                res.devices = body.result;
            })()
        ]);

        const infoMap = new Map();
        for (const i of res.info) {
            infoMap.set(i.device.id, i);
        }

        const fc: FeatureCollection = {
            type: 'FeatureCollection',
            features: res.devices.map((d: any) => {
                let callsign = d.id;
                const metadata: Record<string, string> = {};

                const info = infoMap.get(d.id);
                if (!info) return null;

                if (!d.licenseState) d.licenseState = 'US';
                metadata.vin = d.vehicleIdentificationNumber;
                metadata.licenseState = d.licenseState;
                metadata.licensePlate = d.licensePlate || 'Unknown';
                metadata.groups = info.groups;
                metadata.name = d.name || 'No Name';
                metadata.driver = info.driver || 'No Driver';

                if (typeof info.driver === 'string' && info.driver !== 'UnknownDriverId') {
                    callsign = info.driver
                } else if (d.name) {
                    callsign = d.name;
                } else {
                    callsign = d.licenseState + '-' + (d.licensePlate || 'Unknown')
                }

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

    /**
     * The Login endpoint is rightfully rate limited, I don't believe this endpoint has the same
     * limits so we're using this to test cached credentials
     */
    async user(env: Static<typeof SchemaInput>, credentials: Static<typeof Credentials>) {
        const user_res = await fetch(new URL(env.GEOTAB_API + '/apiv1'), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                method: 'Get',
                params: {
                    credentials,
                    search: { name: credentials.userName },
                    typeName: "User",
                }
            })
        });

        if (!user_res.ok) {
            throw new Error(await user_res.text());
        }

        await user_res.typed(Type.Object({
            result: Type.Array(Type.Object({
                name: Type.String()
            }))
        }));
    }

    async login(env: Static<typeof SchemaInput>): Promise<Static<typeof Credentials>> {
        const url = new URL(env.GEOTAB_API + '/apiv1');
        console.log(`ok - POST ${String(url)}`);

        const auth = await fetch(url, {
            method: 'POST',
            headers: {
                Accept: 'application/json',
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                method: 'Authenticate',
                params: {
                    database: env.GEOTAB_DATABASE || '',
                    userName: env.GEOTAB_USERNAME,
                    password: env.GEOTAB_PASSWORD
                }
            })
        });

        const body = await auth.typed(Type.Object({
            result: Type.Object({
                credentials: Credentials
            })
        }))

        const credentials = body.result.credentials;

        return credentials;
    }

}

env(import.meta.url)
await local(new Task(), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(), event);
}

