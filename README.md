<h1 align='center'>ETL-GeoTAB</h1>

<p align='center'>Bring GeoTAB Location data into the TAK System</p>

## Setup
To determine the GeoTab group to be passed to the ETL, use developer tools and inspect the request payload when selecting the group using the GeoTab UI. A "Group ID" tag should be seen and this will be added to the CloudTAK Environment section when deploying the ETL.  
<img width="631" height="119" alt="image" src="https://github.com/user-attachments/assets/0538f6e6-d145-4a4b-9179-68128542757d" />


## Development

DFPC provided Lambda ETLs are currently all written in [NodeJS](https://nodejs.org/en) through the use of a AWS Lambda optimized
Docker container. Documentation for the Dockerfile can be found in the [AWS Help Center](https://docs.aws.amazon.com/lambda/latest/dg/images-create.html)

```sh
npm install
```

Add a .env file in the root directory that gives the ETL script the necessary variables to communicate with a local ETL server.
When the ETL is deployed the `ETL_API` and `ETL_LAYER` variables will be provided by the Lambda Environment

```json
{
    "ETL_API": "http://localhost:5001",
    "ETL_LAYER": "19"
}
```

To run the task, ensure the local [CloudTAK](https://github.com/dfpc-coe/CloudTAK/) server is running and then run with typescript runtime
or build to JS and run natively with node

```
ts-node task.ts
```

```
npm run build
cp .env dist/
node dist/task.js
```

### Deployment

Deployment into the CloudTAK environment for configuration is done via automatic releases to the DFPC AWS environment.

Github actions will build and push docker releases on every version tag which can then be automatically configured via the
CloudTAK API.

Non-DFPC users will need to setup their own docker => ECS build system via something like Github Actions or AWS Codebuild.


