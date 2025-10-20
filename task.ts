import ETL, { Event, SchemaType, handler as internal, local, fetch, DataFlowType, InvocationType } from '@tak-ps/etl';
import { Feature } from '@tak-ps/node-cot'
import { Type, TSchema, Static } from '@sinclair/typebox';

const Credentials = Type.Object({
    database: Type.String(),
    sessionId: Type.String(),
    userName: Type.String()
})

// This schema is believed to be complete
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
    driver: Type.Optional(Type.Union([Type.String(), Type.Object({
        id: Type.String(),
        isDriver: Type.Boolean(),
        driverGroups: Type.Array(Type.Object({
            id: Type.String()
        }))
    })])),
    isHistoricLastDriver: Type.Boolean(),
    groups: Type.Array(Type.Object({
        id: Type.String()
    }))
});

// This schema is not complete and only grabs relevantish fields
const GEOTAB_Driver = Type.Object({
    id: Type.String(),
    name: Type.String(),
    comment: Type.String(),
    phoneNumber: Type.String(),
    firstName: Type.String(),
    lastName: Type.String(),
    designation: Type.String(),
});

export const Result = Type.Object({
    info: Type.Optional(Type.Array(GEOTAB_DeviceInfo)),

    // TODO Type these
    devices: Type.Optional(Type.Array(Type.Any())),
    drivers: Type.Optional(Type.Array(GEOTAB_Driver))
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

const SchemaOutput = Type.Object({
    vin: Type.String(),
    name: Type.String(),
    licenseState: Type.String(),
    licensePlate: Type.String(),
    groups: Type.Array(Type.String()),
    driverUsername: Type.Optional(Type.String()),
    driverFirstName: Type.Optional(Type.String()),
    driverLastName: Type.Optional(Type.String()),
    driverPhone: Type.Optional(Type.String()),
    driverDesignation: Type.Optional(Type.String()),
    driverComment: Type.Optional(Type.String()),
})

export default class Task extends ETL {
    static name = 'etl-geotab';
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return SchemaInput
            } else {
                return SchemaOutput
            }
        } else {
            return Type.Object({});
        }
    }

    async control() {
        const layer = await this.fetchLayer();
        const env = await this.env(SchemaInput);

        let credentials: Static<typeof Credentials>;
        if (Object.keys(layer.incoming.ephemeral).length) {
            try {
                credentials = layer.incoming.ephemeral as Static<typeof Credentials>;
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
                    result: Type.Array(GEOTAB_Driver)
                }));

                res.drivers = body.result;
            })(),
            (async () => {
                const params: any = {
                    credentials,
                    typeName: "Device",
                    search: {
                        excludeUntrackedAssets: true,
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

        const driverMap = new Map();
        for (const d of res.drivers) {
            driverMap.set(d.id, d);
        }

        const hourAgo = new Date(new Date().getTime() - 3600000)

        const fc: Static<typeof Feature.InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: res.devices.map((d: any) => {
                let callsign = d.id;
                const info = infoMap.get(d.id);
                if (!info) return null;

                const metadata: Static<typeof SchemaOutput> = {
                    vin: d.vehicleIdentificationNumber,
                    licenseState: d.licenseState || 'US',
                    licensePlate: d.licensePlate || 'Unknown',
                    groups: info.groups,
                    name: d.name || 'No Name',
                };


                if (typeof info.driver !== 'string' && info.driver) {
                    const driver = driverMap.get(info.driver.id);

                    if (driver) {
                        metadata.driverUsername = driver.name
                        metadata.driverFirstName = driver.firstName
                        metadata.driverLastName = driver.lastName;
                        metadata.driverPhone = driver.phoneNumber
                        metadata.driverDesignation = driver.designation
                        metadata.driverComment = driver.comment
                    }
                }

                if (d.name) {
                    callsign = d.name;
                } else {
                    callsign = d.licenseState + '-' + (d.licensePlate || 'Unknown')
                }

                if (new Date(info.dateTime) <= hourAgo) {
                    return null
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

                return feat as Static<typeof Feature.InputFeature>;
            }).filter((f: Static<typeof Feature.InputFeature> | null) => {
                return f !== null;
            }).filter((f: Static<typeof Feature.InputFeature>) => {
                // @ts-expect-error Metadata currently isn't typed
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

await local(await Task.init(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(await Task.init(import.meta.url), event);
}

