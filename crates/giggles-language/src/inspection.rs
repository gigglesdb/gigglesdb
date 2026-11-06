//! Standalone discovery through the selected frontend profile or typed catalog.
use crate::{
    CapabilityLimits, CapabilityManifest, Engine, Error, InspectionStatement, LanguageVersion,
    Result, capability_manifest_for, execute, parse_inspection_with_version, parser,
};
use giggles_core::operations;
use serde::Serialize;

/// Local discovery and committed catalog metadata are distinct result shapes.
/// No variant contains an authority credential, prepared handle or receipt.
#[derive(Clone, Debug, Serialize)]
pub enum InspectionOutcome {
    Capabilities(CapabilityManifest),
    Limits(CapabilityLimits),
    Catalog(operations::InspectionOutcome),
}

pub(crate) fn validate(statement: &InspectionStatement, version: LanguageVersion) -> Result<()> {
    if !version.supports_inspection() {
        return Err(Error::Unsupported);
    }
    if let InspectionStatement::DescribeTable { table } = statement {
        if table.len() > 63 {
            return Err(Error::Limit);
        }
        if !parser::semantic_identifier(table, version) {
            return Err(Error::Bind("identifier".into()));
        }
    }
    Ok(())
}

impl Engine {
    /// Inspect one standalone command; no bindings, ambient transaction or retry.
    pub fn inspect_text(&self, text: &str) -> Result<InspectionOutcome> {
        self.inspect(&parse_inspection_with_version(text, self.version)?)
    }

    /// Catalog results use a fresh committed snapshot on every call.
    /// Capability and limit discovery is local to the selected language profile.
    pub fn inspect(&self, statement: &InspectionStatement) -> Result<InspectionOutcome> {
        validate(statement, self.version)?;
        let request = match statement {
            InspectionStatement::ShowCapabilities => {
                return Ok(InspectionOutcome::Capabilities(capability_manifest_for(
                    self.version,
                )));
            }
            InspectionStatement::ShowLimits => {
                return Ok(InspectionOutcome::Limits(
                    capability_manifest_for(self.version).limits,
                ));
            }
            InspectionStatement::ShowTables => operations::InspectionRequest::Tables,
            InspectionStatement::DescribeTable { table } => operations::InspectionRequest::Table {
                table: table.clone(),
            },
        };
        self.inner
            .inspect(&request)
            .map(InspectionOutcome::Catalog)
            .map_err(execute::error)
    }
}
