import os

import uvicorn
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware

from routes import aas_discovery, shell_registry, shell_repo, submodel_registry, submodel_repo


class Api:
    def __init__(self):
        app = FastAPI(
            title="Information Receiving Service",
            openapi_url="/api/v1/openapi.json",
            swagger_ui_parameters={
                "displayOperationId": False
            }
        )
        app.add_middleware(GZipMiddleware, minimum_size=1000)
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        for router in self.router:
            app.include_router(router)

        uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("SERVICE_PORT")))

    @property
    def router(self):
        return [
            aas_discovery.router,
            shell_registry.router,
            shell_repo.router,
            submodel_registry.router,
            submodel_repo.router
        ]


