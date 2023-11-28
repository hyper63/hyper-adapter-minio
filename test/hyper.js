// Load .env
import 'https://deno.land/std@0.208.0/dotenv/load.ts'

import { default as app } from 'https://raw.githubusercontent.com/hyper63/hyper/hyper-app-express%40v1.2.1/packages/app-express/mod.ts'
import { default as core } from 'https://raw.githubusercontent.com/hyper63/hyper/hyper%40v4.3.1/packages/core/mod.ts'

import myAdapter from '../mod.js'
import PORT_NAME from '../port_name.js'

const hyperConfig = {
  app,
  adapters: [
    {
      port: PORT_NAME,
      plugins: [myAdapter({ url: Deno.env.get('MINIO_URL'), bucketPrefix: 'harness' })],
    },
  ],
}

core(hyperConfig)
