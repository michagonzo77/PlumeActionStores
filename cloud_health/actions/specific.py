from pydantic import BaseModel, constr
from . import action_store as action_store
import logging

logger = logging.getLogger(__name__)



class ProductionCloudHealthRequest(BaseModel):
    cloud_location: constr(regex='^(gamma|kappa)$')

class ProductionCloudHealthResponse(BaseModel):
    cloud_link: str

@action_store.kubiya_action()
def production_cloud_health(request: ProductionCloudHealthRequest) -> ProductionCloudHealthResponse:
    if request.cloud_location == "gamma":
        return ProductionCloudHealthResponse(cloud_link="https://grafana.sso.plumenet.io/d/gamma-mocha-and-pytests/mocha-and-pytests")
    elif request.cloud_location == "kappa":
        return ProductionCloudHealthResponse(cloud_link="https://grafana.sso.plumenet.io/d/kappa-mocha-and-pytests/mocha-and-pytests")
    


class DevelopmentCloudHealthRequest(BaseModel):
    cloud_location: constr(regex='^(dogfood|opensync|osync|thetadev|ci|func4)$')

class DevelopmentCloudHealthResponse(BaseModel):
    cloud_link: str


@action_store.kubiya_action()
def development_cloud_health(request: DevelopmentCloudHealthRequest) -> DevelopmentCloudHealthResponse:
    if request.cloud_location == "dogfood":
        return DevelopmentCloudHealthResponse(cloud_link="https://grafana.sso.plume.tech/d/dogfood-mocha-and-pytests/mocha-and-pytests")
    elif request.cloud_location == "opensync":
        return DevelopmentCloudHealthResponse(cloud_link="https://grafana.sso.plume.tech/d/opensync-mocha-and-pytests/mocha-and-pytests")
    elif request.cloud_location == "osync":
        return DevelopmentCloudHealthResponse(cloud_link="https://grafana.sso.plume.tech/d/osync-mocha-and-pytests/mocha-and-pytests")
    elif request.cloud_location == "thetadev":
        return DevelopmentCloudHealthResponse(cloud_link="https://grafana.sso.plume.tech/d/thetadev-mocha-and-pytests/mocha-and-pytests")
    elif request.cloud_location == "ci":
        return DevelopmentCloudHealthResponse(cloud_link="https://grafana.sso.plume.tech/d/ci-mocha-and-pytests/mocha-and-pytests")
    elif request.cloud_location == "func4":
        return DevelopmentCloudHealthResponse(cloud_link="https://grafana.sso.plume.tech/d/func4-mocha-and-pytests/mocha-and-pytests")