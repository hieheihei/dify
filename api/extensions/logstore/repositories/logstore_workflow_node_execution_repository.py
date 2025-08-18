"""
Logstore implementation of the WorkflowNodeExecutionRepository.
"""

import json
import logging
from datetime import datetime
from typing import Optional, Union

from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from core.workflow.entities.workflow_node_execution import (
    WorkflowNodeExecution,
    WorkflowNodeExecutionMetadataKey,
    WorkflowNodeExecutionStatus,
)
from core.workflow.nodes.enums import NodeType
from core.workflow.repositories.workflow_node_execution_repository import OrderConfig, WorkflowNodeExecutionRepository
from extensions.logstore.aliyun_logstore import AliyunLogStore
from libs.helper import extract_tenant_id
from models import (
    Account,
    CreatorUserRole,
    EndUser,
    WorkflowNodeExecutionTriggeredFrom,
)

logger = logging.getLogger(__name__)


class LogstoreWorkflowNodeExecutionRepository(WorkflowNodeExecutionRepository):
    """
    Logstore implementation of the WorkflowNodeExecutionRepository interface.

    This implementation supports multi-tenancy by filtering operations based on tenant_id.
    Data is stored in Aliyun SLS logstore as key-value pairs.
    """

    def __init__(
        self,
        session_factory: sessionmaker | Engine,
        user: Union[Account, EndUser],
        app_id: Optional[str],
        triggered_from: Optional[WorkflowNodeExecutionTriggeredFrom],
    ):
        """
        Initialize the repository with a SQLAlchemy sessionmaker or engine and context information.

        Args:
            session_factory: SQLAlchemy sessionmaker or engine for creating sessions
            user: Account or EndUser object containing tenant_id, user ID, and role information
            app_id: App ID for filtering by application (can be None)
            triggered_from: Source of the execution trigger (SINGLE_STEP or WORKFLOW_RUN)
        """
        self.logstore_client = AliyunLogStore()

        # Extract tenant_id from user
        tenant_id = extract_tenant_id(user)
        if not tenant_id:
            raise ValueError("User must have a tenant_id or current_tenant_id")
        self._tenant_id = tenant_id

        # Store app context
        self._app_id = app_id

        # Extract user context
        self._triggered_from = triggered_from
        self._creator_user_id = user.id

        # Determine user role based on user type
        self._creator_user_role = CreatorUserRole.ACCOUNT if isinstance(user, Account) else CreatorUserRole.END_USER

    def _to_domain_model(self, logstore_model: list[tuple[str, str]]) -> WorkflowNodeExecution:
        """
        Convert a logstore model (List[Tuple[str, str]]) to a domain model.

        Args:
            logstore_model: The logstore model as a list of key-value tuples

        Returns:
            The domain model
        """
        # Convert list of tuples to dictionary for easier access
        logstore_dict = dict(logstore_model)

        # Parse JSON fields
        inputs = json.loads(logstore_dict.get('inputs', '{}'))
        process_data = json.loads(logstore_dict.get('process_data', '{}'))
        outputs = json.loads(logstore_dict.get('outputs', '{}'))
        metadata = json.loads(logstore_dict.get('execution_metadata', '{}'))

        # Convert metadata to domain enum keys
        domain_metadata = {}
        for k, v in metadata.items():
            try:
                domain_metadata[WorkflowNodeExecutionMetadataKey(k)] = v
            except ValueError:
                # Skip invalid metadata keys
                continue

        # Convert status to domain enum
        status = WorkflowNodeExecutionStatus(logstore_dict.get('status', 'running'))

        # Parse datetime fields
        created_at = datetime.fromisoformat(logstore_dict.get('created_at', ''))\
            if logstore_dict.get('created_at') else datetime.now()
        finished_at = datetime.fromisoformat(logstore_dict.get('finished_at', ''))\
            if logstore_dict.get('finished_at') else None

        return WorkflowNodeExecution(
            id=logstore_dict.get('id', ''),
            node_execution_id=logstore_dict.get('node_execution_id'),
            workflow_id=logstore_dict.get('workflow_id', ''),
            workflow_execution_id=logstore_dict.get('workflow_run_id'),
            index=int(logstore_dict.get('index', 0)),
            predecessor_node_id=logstore_dict.get('predecessor_node_id'),
            node_id=logstore_dict.get('node_id', ''),
            node_type=NodeType(logstore_dict.get('node_type', 'start')),
            title=logstore_dict.get('title', ''),
            inputs=inputs,
            process_data=process_data,
            outputs=outputs,
            status=status,
            error=logstore_dict.get('error'),
            elapsed_time=float(logstore_dict.get('elapsed_time', 0.0)),
            metadata=domain_metadata,
            created_at=created_at,
            finished_at=finished_at,
        )

    def _to_logstore_model(self, domain_model: WorkflowNodeExecution) -> list[tuple[str, str]]:
        """
        Convert a domain model to a logstore model (List[Tuple[str, str]]).

        Args:
            domain_model: The domain model to convert

        Returns:
            The logstore model as a list of key-value tuples
        """
        # Use values from constructor if provided
        if not self._triggered_from:
            raise ValueError("triggered_from is required in repository constructor")
        if not self._creator_user_id:
            raise ValueError("created_by is required in repository constructor")
        if not self._creator_user_role:
            raise ValueError("created_by_role is required in repository constructor")

        logstore_model = [
            ('id', domain_model.id),
            ('tenant_id', self._tenant_id),
            ('app_id', self._app_id or ''),
            ('workflow_id', domain_model.workflow_id),
            ('triggered_from', self._triggered_from.value
                if hasattr(self._triggered_from, 'value') else str(self._triggered_from)),
            ('workflow_run_id', domain_model.workflow_execution_id or ''),
            ('index', str(domain_model.index)),
            ('predecessor_node_id', domain_model.predecessor_node_id or ''),
            ('node_execution_id', domain_model.node_execution_id or ''),
            ('node_id', domain_model.node_id),
            ('node_type', domain_model.node_type.value),
            ('title', domain_model.title),
            ('inputs', json.dumps(domain_model.inputs) if domain_model.inputs else '{}'),
            ('process_data', json.dumps(domain_model.process_data) if domain_model.process_data else '{}'),
            ('outputs', json.dumps(domain_model.outputs) if domain_model.outputs else '{}'),
            ('status', domain_model.status.value),
            ('error', domain_model.error or ''),
            ('elapsed_time', str(domain_model.elapsed_time)),
            ('execution_metadata', json.dumps(domain_model.metadata) if domain_model.metadata else '{}'),
            ('created_at', domain_model.created_at.isoformat() if domain_model.created_at else ''),
            ('created_by_role', self._creator_user_role.value
                if hasattr(self._creator_user_role, 'value') else str(self._creator_user_role)),
            ('created_by', self._creator_user_id),
            ('finished_at', domain_model.finished_at.isoformat() if domain_model.finished_at else ''),
        ]

        return logstore_model

    def save(self, execution: WorkflowNodeExecution) -> None:
        """
        Save or update a NodeExecution domain entity to the logstore.

        This method serves as a domain-to-logstore adapter that:
        1. Converts the domain entity to its logstore representation
        2. Persists the logstore model using Aliyun SLS
        3. Maintains proper multi-tenancy by including tenant context during conversion

        Args:
            execution: The NodeExecution domain entity to persist
        """
        # Convert domain model to logstore model using tenant context and other attributes
        logstore_model = self._to_logstore_model(execution)

        # Store in logstore
        self.logstore_client.put_log(AliyunLogStore.workflow_node_execution_logstore, logstore_model)

        logger.debug("Saved node execution to logstore for node_execution_id: %s", execution.node_execution_id)

    def get_db_models_by_workflow_run(
        self,
        workflow_run_id: str,
        order_config: Optional[OrderConfig] = None,
    ) -> list[list[tuple[str, str]]]:
        """
        Retrieve all WorkflowNodeExecution logstore models for a specific workflow run.

        Note: This method is not implemented as it requires complex logstore querying capabilities.
        In a real implementation, you would need to implement logstore-specific querying logic.

        Args:
            workflow_run_id: The workflow run ID
            order_config: Optional configuration for ordering results

        Returns:
            A list of logstore models (List[List[Tuple[str, str]]])
        """
        # TODO: Implement logstore querying logic
        # This would require implementing search/filter capabilities in the logstore client
        logger.warning("get_db_models_by_workflow_run not implemented for logstore repository")
        return []

    def get_by_workflow_run(
        self,
        workflow_run_id: str,
        order_config: Optional[OrderConfig] = None,
    ) -> list[WorkflowNodeExecution]:
        """
        Retrieve all NodeExecution instances for a specific workflow run.

        Note: This method is not implemented as it requires complex logstore querying capabilities.
        In a real implementation, you would need to implement logstore-specific querying logic.

        Args:
            workflow_run_id: The workflow run ID
            order_config: Optional configuration for ordering results

        Returns:
            A list of NodeExecution instances
        """
        # TODO: Implement logstore querying logic
        # This would require implementing search/filter capabilities in the logstore client
        logger.warning("get_by_workflow_run not implemented for logstore repository")
        return []
