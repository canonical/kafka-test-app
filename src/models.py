# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Class to handle configuration and relation databag."""
from enum import Enum
from typing import List, Literal, Optional

from charms.data_platform_libs.v0.data_interfaces import Data, RequirerData
from charms.data_platform_libs.v0.data_models import RelationDataModel
from ops.model import Application, Relation, Unit
from pydantic import BaseModel


class BaseEnumStr(str, Enum):
    """Base class for string enum."""

    def __str__(self) -> str:
        """Return the value as a string."""
        return str(self.value)


class AppType(BaseEnumStr):
    """Class for the app type."""

    PRODUCER = "producer"
    CONSUMER = "consumer"
    ADMIN = "admin"


class RelationContext:
    """Relation context object."""

    def __init__(
        self,
        relation: Relation | None,
        data_interface: Data,
        component: Unit | Application | None,
    ):
        self.relation = relation
        self.data_interface = data_interface
        self.component = component
        self.relation_data = self.data_interface.as_dict(self.relation.id) if self.relation else {}

    def __bool__(self) -> bool:
        """Boolean evaluation based on the existence of self.relation."""
        try:
            return bool(self.relation)
        except AttributeError:
            return False

    def update(self, items: dict[str, str]) -> None:
        """Writes to relation_data."""
        delete_fields = [key for key in items if not items[key]]
        update_content = {k: items[k] for k in items if k not in delete_fields}
        self.relation_data.update(update_content)
        for field in delete_fields:
            del self.relation_data[field]


class CharmConfig(BaseModel):
    """Charmed configuration class."""

    topic_name: str
    role: Literal["consumer", "producer", "admin"]
    replication_factor: int
    consumer_group_prefix: Optional[str] = None
    partitions: int
    num_messages: int


class StartConsumerParam(BaseModel):
    """Class to handle consumer parameters."""

    consumer_group_prefix: Optional[str]


class KafkaRequirerRelationDataBag(BaseModel):
    """Class for Kafka relation data."""

    topic: str
    extra_user_roles: str

    @property
    def app_type(self) -> List[AppType]:
        """Return the app types."""
        return [AppType(value) for value in self.extra_user_roles.split(",")]


class AuthDataBag(RelationContext):
    """Class to handle authentication parameters."""

    def __init__(
        self,
        relation: Relation | None,
        data_interface: RequirerData,
    ):
        super().__init__(relation, data_interface, None)

    @property
    def username(self) -> str:
        """Returns the provider client username."""
        return self.relation_data.get("username", "")

    @property
    def password(self) -> str:
        """Returns the provider client password."""
        return self.relation_data.get("password", "")

    @property
    def endpoints(self) -> str:
        """Returns provider endpoints."""
        return self.relation_data.get("endpoints", "")

    @property
    def tls(self) -> bool:
        """Returns True if TLS is enabled on relation."""
        tls = self.relation_data.get("tls")

        if tls is not None and tls != "disabled":
            return True

        return False

    @property
    def tls_ca(self) -> str | None:
        """Returns the provider CA if the relation uses TLS, otherwise empty string."""
        return self.relation_data.get("tls-ca", "")


class KafkaProviderRelationDataBag(AuthDataBag):
    """Class for the provider relation databag."""

    @property
    def consumer_group_prefix(self) -> str | None:
        """Return the consumer group prefix."""
        return self.relation_data.get("consumer-group-prefix")

    @property
    def security_protocol(self) -> str:
        """Return the security protocol."""
        return "SASL_PLAINTEXT" if self.tls is not None else "SASL_SSL"

    @property
    def bootstrap_server(self) -> str:
        """Return the bootstrap servers endpoints."""
        return self.endpoints


class MongoProviderRelationDataBag(AuthDataBag):
    """Class that handle the MongoDB relation databag."""

    read_only_endpoints: Optional[str]
    replset: Optional[str]
    uris: Optional[str]
    version: Optional[str]


class PeerRelationAppData(RelationDataModel):
    """Class to handle the peer relation databag."""

    topic_name: Optional[str] = None
    database_name: Optional[str] = None


class PeerRelationUnitData(RelationDataModel):
    """Class to handle the unit peer relation databag."""

    pid: Optional[int]
    private_key: Optional[str] = None
