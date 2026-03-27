from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field


RequirementCategory = Literal[
    "functional",
    "non_functional",
    "constraint",
    "assumption",
    "open_question",
    "acceptance",
    "integration",
    "data",
    "security",
]
RequirementPriority = Literal["critical", "high", "medium", "low"]
PhaseVerdict = Literal["pass", "pass_with_notes", "fail"]
AcceptanceVerdict = Literal["accepted", "rejected"]
RunStatus = Literal["initialized", "in_progress", "blocked", "completed", "failed"]


class HarnessModel(BaseModel):
    """Strict pydantic model base for harness canonical artifacts."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class ArtifactManifestEntryModel(HarnessModel):
    artifact_id: str = Field(min_length=1)
    path: str = Field(min_length=1)
    sha256: str = Field(min_length=64, max_length=64, pattern=r"^[a-f0-9]{64}$")
    size_bytes: int = Field(ge=0)
    media_type: str = Field(min_length=1)
    extraction_status: Literal["pending", "extracted", "unsupported", "failed"]
    source_kind: Literal["zip", "file", "directory"]


class SpecManifestModel(HarnessModel):
    run_id: str = Field(min_length=1)
    generated_at: str = Field(min_length=1)
    artifacts: list[ArtifactManifestEntryModel] = Field(min_length=1)


class NormalizedRequirementModel(HarnessModel):
    requirement_id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    statement: str = Field(min_length=1)
    category: RequirementCategory
    priority: RequirementPriority
    phase_hint: str | None = None
    source_refs: list[str] = Field(default_factory=list)
    ambiguity_flag: bool
    acceptance_hint: str | None = None
    depends_on: list[str] = Field(default_factory=list)


class NormalizedRequirementsModel(HarnessModel):
    run_id: str = Field(min_length=1)
    generated_at: str = Field(min_length=1)
    requirements: list[NormalizedRequirementModel] = Field(default_factory=list)


class DomainGlossaryEntryModel(HarnessModel):
    term: str = Field(min_length=1)
    definition: str = Field(min_length=1)


class RiskRegisterEntryModel(HarnessModel):
    risk_id: str = Field(min_length=1)
    statement: str = Field(min_length=1)
    mitigation: str | None = None


class ProjectDocsBundleModel(HarnessModel):
    run_id: str = Field(min_length=1)
    generated_at: str = Field(min_length=1)
    problem_statement: str = Field(min_length=1)
    scope_in: list[str] = Field(default_factory=list)
    scope_out: list[str] = Field(default_factory=list)
    actors: list[str] = Field(default_factory=list)
    business_flows: list[str] = Field(default_factory=list)
    domain_glossary: list[DomainGlossaryEntryModel] = Field(default_factory=list)
    data_objects: list[str] = Field(default_factory=list)
    integration_points: list[str] = Field(default_factory=list)
    nfrs: list[str] = Field(default_factory=list)
    architecture_constraints: list[str] = Field(default_factory=list)
    design_decisions: list[str] = Field(default_factory=list)
    risk_register: list[RiskRegisterEntryModel] = Field(default_factory=list)
    open_questions: list[str] = Field(default_factory=list)
    assumptions: list[str] = Field(default_factory=list)
    adr_seeds: list[str] = Field(default_factory=list)
    implementation_boundaries: list[str] = Field(default_factory=list)


class PhaseDefinitionModel(HarnessModel):
    phase_id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    goal: str = Field(min_length=1)
    scope_in: list[str] = Field(default_factory=list)
    scope_out: list[str] = Field(default_factory=list)
    requirement_ids: list[str] = Field(default_factory=list)
    dependencies: list[str] = Field(default_factory=list)
    done_definition: list[str] = Field(default_factory=list)
    acceptance_checks: list[str] = Field(default_factory=list)
    required_tests: list[str] = Field(default_factory=list)
    risk_notes: list[str] = Field(default_factory=list)
    doc_outputs: list[str] = Field(default_factory=list)
    allowed_change_surfaces: list[str] = Field(default_factory=list)


class PhasePlanModel(HarnessModel):
    run_id: str = Field(min_length=1)
    generated_at: str = Field(min_length=1)
    phases: list[PhaseDefinitionModel] = Field(default_factory=list)


class DocExcerptModel(HarnessModel):
    title: str = Field(min_length=1)
    content: str = Field(min_length=1)


class PriorFindingModel(HarnessModel):
    source: Literal["review", "acceptance"]
    summary: str = Field(min_length=1)
    severity: Literal["low", "medium", "high", "critical"]


class PhaseContextModel(HarnessModel):
    run_id: str = Field(min_length=1)
    phase_id: str = Field(min_length=1)
    generated_at: str = Field(min_length=1)
    requirement_ids: list[str] = Field(default_factory=list)
    requirements: list[NormalizedRequirementModel] = Field(default_factory=list)
    doc_excerpts: list[DocExcerptModel] = Field(default_factory=list)
    risk_notes: list[str] = Field(default_factory=list)
    open_questions: list[str] = Field(default_factory=list)
    done_definition: list[str] = Field(default_factory=list)
    acceptance_checks: list[str] = Field(default_factory=list)
    allowed_change_surfaces: list[str] = Field(default_factory=list)
    test_scope: list[str] = Field(default_factory=list)
    prior_findings: list[PriorFindingModel] = Field(default_factory=list)


class RunStateModel(HarnessModel):
    run_id: str = Field(min_length=1)
    created_at: str = Field(min_length=1)
    updated_at: str = Field(min_length=1)
    status: RunStatus
    current_phase_id: str | None = None
    accepted_phase_ids: list[str] = Field(default_factory=list)
    rejected_phase_ids: list[str] = Field(default_factory=list)
    iteration_count: int = Field(ge=0, default=0)
    last_event_sequence: int = Field(ge=0, default=0)


@dataclass(frozen=True)
class ArtifactManifestEntry:
    artifact_id: str
    path: str
    sha256: str
    size_bytes: int
    media_type: str
    extraction_status: str
    source_kind: str

    def to_model(self) -> ArtifactManifestEntryModel:
        return ArtifactManifestEntryModel(
            artifact_id=self.artifact_id,
            path=self.path,
            sha256=self.sha256,
            size_bytes=self.size_bytes,
            media_type=self.media_type,
            extraction_status=self.extraction_status,
            source_kind=self.source_kind,
        )

    @classmethod
    def from_model(cls, model: ArtifactManifestEntryModel) -> "ArtifactManifestEntry":
        payload = model.model_dump()
        return cls(**payload)


@dataclass(frozen=True)
class NormalizedRequirement:
    requirement_id: str
    title: str
    statement: str
    category: RequirementCategory
    priority: RequirementPriority
    phase_hint: str | None
    source_refs: tuple[str, ...]
    ambiguity_flag: bool
    acceptance_hint: str | None
    depends_on: tuple[str, ...]

    def to_model(self) -> NormalizedRequirementModel:
        return NormalizedRequirementModel(
            requirement_id=self.requirement_id,
            title=self.title,
            statement=self.statement,
            category=self.category,
            priority=self.priority,
            phase_hint=self.phase_hint,
            source_refs=list(self.source_refs),
            ambiguity_flag=self.ambiguity_flag,
            acceptance_hint=self.acceptance_hint,
            depends_on=list(self.depends_on),
        )

    @classmethod
    def from_model(cls, model: NormalizedRequirementModel) -> "NormalizedRequirement":
        return cls(
            requirement_id=model.requirement_id,
            title=model.title,
            statement=model.statement,
            category=model.category,
            priority=model.priority,
            phase_hint=model.phase_hint,
            source_refs=tuple(model.source_refs),
            ambiguity_flag=model.ambiguity_flag,
            acceptance_hint=model.acceptance_hint,
            depends_on=tuple(model.depends_on),
        )


@dataclass(frozen=True)
class DomainGlossaryEntry:
    term: str
    definition: str

    def to_model(self) -> DomainGlossaryEntryModel:
        return DomainGlossaryEntryModel(term=self.term, definition=self.definition)

    @classmethod
    def from_model(cls, model: DomainGlossaryEntryModel) -> "DomainGlossaryEntry":
        return cls(term=model.term, definition=model.definition)


@dataclass(frozen=True)
class RiskRegisterEntry:
    risk_id: str
    statement: str
    mitigation: str | None = None

    def to_model(self) -> RiskRegisterEntryModel:
        return RiskRegisterEntryModel(
            risk_id=self.risk_id,
            statement=self.statement,
            mitigation=self.mitigation,
        )

    @classmethod
    def from_model(cls, model: RiskRegisterEntryModel) -> "RiskRegisterEntry":
        return cls(risk_id=model.risk_id, statement=model.statement, mitigation=model.mitigation)


@dataclass(frozen=True)
class ProjectDocsBundle:
    run_id: str
    generated_at: str
    problem_statement: str
    scope_in: tuple[str, ...]
    scope_out: tuple[str, ...]
    actors: tuple[str, ...]
    business_flows: tuple[str, ...]
    domain_glossary: tuple[DomainGlossaryEntry, ...]
    data_objects: tuple[str, ...]
    integration_points: tuple[str, ...]
    nfrs: tuple[str, ...]
    architecture_constraints: tuple[str, ...]
    design_decisions: tuple[str, ...]
    risk_register: tuple[RiskRegisterEntry, ...]
    open_questions: tuple[str, ...]
    assumptions: tuple[str, ...]
    adr_seeds: tuple[str, ...]
    implementation_boundaries: tuple[str, ...]

    def to_model(self) -> ProjectDocsBundleModel:
        return ProjectDocsBundleModel(
            run_id=self.run_id,
            generated_at=self.generated_at,
            problem_statement=self.problem_statement,
            scope_in=list(self.scope_in),
            scope_out=list(self.scope_out),
            actors=list(self.actors),
            business_flows=list(self.business_flows),
            domain_glossary=[item.to_model() for item in self.domain_glossary],
            data_objects=list(self.data_objects),
            integration_points=list(self.integration_points),
            nfrs=list(self.nfrs),
            architecture_constraints=list(self.architecture_constraints),
            design_decisions=list(self.design_decisions),
            risk_register=[item.to_model() for item in self.risk_register],
            open_questions=list(self.open_questions),
            assumptions=list(self.assumptions),
            adr_seeds=list(self.adr_seeds),
            implementation_boundaries=list(self.implementation_boundaries),
        )

    @classmethod
    def from_model(cls, model: ProjectDocsBundleModel) -> "ProjectDocsBundle":
        return cls(
            run_id=model.run_id,
            generated_at=model.generated_at,
            problem_statement=model.problem_statement,
            scope_in=tuple(model.scope_in),
            scope_out=tuple(model.scope_out),
            actors=tuple(model.actors),
            business_flows=tuple(model.business_flows),
            domain_glossary=tuple(DomainGlossaryEntry.from_model(item) for item in model.domain_glossary),
            data_objects=tuple(model.data_objects),
            integration_points=tuple(model.integration_points),
            nfrs=tuple(model.nfrs),
            architecture_constraints=tuple(model.architecture_constraints),
            design_decisions=tuple(model.design_decisions),
            risk_register=tuple(RiskRegisterEntry.from_model(item) for item in model.risk_register),
            open_questions=tuple(model.open_questions),
            assumptions=tuple(model.assumptions),
            adr_seeds=tuple(model.adr_seeds),
            implementation_boundaries=tuple(model.implementation_boundaries),
        )


@dataclass(frozen=True)
class PhaseDefinition:
    phase_id: str
    name: str
    goal: str
    scope_in: tuple[str, ...]
    scope_out: tuple[str, ...]
    requirement_ids: tuple[str, ...]
    dependencies: tuple[str, ...]
    done_definition: tuple[str, ...]
    acceptance_checks: tuple[str, ...]
    required_tests: tuple[str, ...]
    risk_notes: tuple[str, ...]
    doc_outputs: tuple[str, ...]
    allowed_change_surfaces: tuple[str, ...]

    def to_model(self) -> PhaseDefinitionModel:
        return PhaseDefinitionModel(
            phase_id=self.phase_id,
            name=self.name,
            goal=self.goal,
            scope_in=list(self.scope_in),
            scope_out=list(self.scope_out),
            requirement_ids=list(self.requirement_ids),
            dependencies=list(self.dependencies),
            done_definition=list(self.done_definition),
            acceptance_checks=list(self.acceptance_checks),
            required_tests=list(self.required_tests),
            risk_notes=list(self.risk_notes),
            doc_outputs=list(self.doc_outputs),
            allowed_change_surfaces=list(self.allowed_change_surfaces),
        )

    @classmethod
    def from_model(cls, model: PhaseDefinitionModel) -> "PhaseDefinition":
        return cls(
            phase_id=model.phase_id,
            name=model.name,
            goal=model.goal,
            scope_in=tuple(model.scope_in),
            scope_out=tuple(model.scope_out),
            requirement_ids=tuple(model.requirement_ids),
            dependencies=tuple(model.dependencies),
            done_definition=tuple(model.done_definition),
            acceptance_checks=tuple(model.acceptance_checks),
            required_tests=tuple(model.required_tests),
            risk_notes=tuple(model.risk_notes),
            doc_outputs=tuple(model.doc_outputs),
            allowed_change_surfaces=tuple(model.allowed_change_surfaces),
        )


@dataclass(frozen=True)
class DocExcerpt:
    title: str
    content: str

    def to_model(self) -> DocExcerptModel:
        return DocExcerptModel(title=self.title, content=self.content)

    @classmethod
    def from_model(cls, model: DocExcerptModel) -> "DocExcerpt":
        return cls(title=model.title, content=model.content)


@dataclass(frozen=True)
class PriorFinding:
    source: Literal["review", "acceptance"]
    summary: str
    severity: Literal["low", "medium", "high", "critical"]

    def to_model(self) -> PriorFindingModel:
        return PriorFindingModel(source=self.source, summary=self.summary, severity=self.severity)

    @classmethod
    def from_model(cls, model: PriorFindingModel) -> "PriorFinding":
        return cls(source=model.source, summary=model.summary, severity=model.severity)


@dataclass(frozen=True)
class PhaseContext:
    run_id: str
    phase_id: str
    generated_at: str
    requirement_ids: tuple[str, ...]
    requirements: tuple[NormalizedRequirement, ...]
    doc_excerpts: tuple[DocExcerpt, ...]
    risk_notes: tuple[str, ...]
    open_questions: tuple[str, ...]
    done_definition: tuple[str, ...]
    acceptance_checks: tuple[str, ...]
    allowed_change_surfaces: tuple[str, ...]
    test_scope: tuple[str, ...]
    prior_findings: tuple[PriorFinding, ...]

    def to_model(self) -> PhaseContextModel:
        return PhaseContextModel(
            run_id=self.run_id,
            phase_id=self.phase_id,
            generated_at=self.generated_at,
            requirement_ids=list(self.requirement_ids),
            requirements=[item.to_model() for item in self.requirements],
            doc_excerpts=[item.to_model() for item in self.doc_excerpts],
            risk_notes=list(self.risk_notes),
            open_questions=list(self.open_questions),
            done_definition=list(self.done_definition),
            acceptance_checks=list(self.acceptance_checks),
            allowed_change_surfaces=list(self.allowed_change_surfaces),
            test_scope=list(self.test_scope),
            prior_findings=[item.to_model() for item in self.prior_findings],
        )

    @classmethod
    def from_model(cls, model: PhaseContextModel) -> "PhaseContext":
        return cls(
            run_id=model.run_id,
            phase_id=model.phase_id,
            generated_at=model.generated_at,
            requirement_ids=tuple(model.requirement_ids),
            requirements=tuple(NormalizedRequirement.from_model(item) for item in model.requirements),
            doc_excerpts=tuple(DocExcerpt.from_model(item) for item in model.doc_excerpts),
            risk_notes=tuple(model.risk_notes),
            open_questions=tuple(model.open_questions),
            done_definition=tuple(model.done_definition),
            acceptance_checks=tuple(model.acceptance_checks),
            allowed_change_surfaces=tuple(model.allowed_change_surfaces),
            test_scope=tuple(model.test_scope),
            prior_findings=tuple(PriorFinding.from_model(item) for item in model.prior_findings),
        )


@dataclass(frozen=True)
class RunState:
    run_id: str
    created_at: str
    updated_at: str
    status: RunStatus
    current_phase_id: str | None
    accepted_phase_ids: tuple[str, ...]
    rejected_phase_ids: tuple[str, ...]
    iteration_count: int
    last_event_sequence: int

    def to_model(self) -> RunStateModel:
        return RunStateModel(
            run_id=self.run_id,
            created_at=self.created_at,
            updated_at=self.updated_at,
            status=self.status,
            current_phase_id=self.current_phase_id,
            accepted_phase_ids=list(self.accepted_phase_ids),
            rejected_phase_ids=list(self.rejected_phase_ids),
            iteration_count=self.iteration_count,
            last_event_sequence=self.last_event_sequence,
        )

    @classmethod
    def from_model(cls, model: RunStateModel) -> "RunState":
        return cls(
            run_id=model.run_id,
            created_at=model.created_at,
            updated_at=model.updated_at,
            status=model.status,
            current_phase_id=model.current_phase_id,
            accepted_phase_ids=tuple(model.accepted_phase_ids),
            rejected_phase_ids=tuple(model.rejected_phase_ids),
            iteration_count=model.iteration_count,
            last_event_sequence=model.last_event_sequence,
        )


def parse_spec_manifest(payload: Mapping[str, object]) -> SpecManifestModel:
    return SpecManifestModel.model_validate(dict(payload))


def parse_normalized_requirements(payload: Mapping[str, object]) -> NormalizedRequirementsModel:
    return NormalizedRequirementsModel.model_validate(dict(payload))


def parse_project_docs_bundle(payload: Mapping[str, object]) -> ProjectDocsBundleModel:
    return ProjectDocsBundleModel.model_validate(dict(payload))


def parse_phase_plan(payload: Mapping[str, object]) -> PhasePlanModel:
    return PhasePlanModel.model_validate(dict(payload))


def parse_phase_context(payload: Mapping[str, object]) -> PhaseContextModel:
    return PhaseContextModel.model_validate(dict(payload))


def parse_run_state(payload: Mapping[str, object]) -> RunStateModel:
    return RunStateModel.model_validate(dict(payload))


__all__ = [
    "AcceptanceVerdict",
    "ArtifactManifestEntry",
    "ArtifactManifestEntryModel",
    "DocExcerpt",
    "DocExcerptModel",
    "DomainGlossaryEntry",
    "DomainGlossaryEntryModel",
    "HarnessModel",
    "NormalizedRequirement",
    "NormalizedRequirementModel",
    "NormalizedRequirementsModel",
    "PhaseContext",
    "PhaseContextModel",
    "ProjectDocsBundle",
    "ProjectDocsBundleModel",
    "PhaseDefinition",
    "PhaseDefinitionModel",
    "PhasePlanModel",
    "PhaseVerdict",
    "PriorFinding",
    "PriorFindingModel",
    "RiskRegisterEntry",
    "RiskRegisterEntryModel",
    "RequirementCategory",
    "RequirementPriority",
    "RunState",
    "RunStateModel",
    "RunStatus",
    "SpecManifestModel",
    "parse_normalized_requirements",
    "parse_phase_context",
    "parse_project_docs_bundle",
    "parse_phase_plan",
    "parse_run_state",
    "parse_spec_manifest",
]
