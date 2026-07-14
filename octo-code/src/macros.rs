//! Boilerplate-cutting macros, mirroring kaeru's `mem_tool!` / `chain_tools!`.
//!
//! [`file_tool!`] generates a stateless rig `Tool` (errors-as-data) from a name,
//! description, args type, JSON params, and a body: it resolves the workspace
//! root and injects it as `$root`, so each tool is a few lines instead of a full
//! trait impl. [`code_tools!`] registers the whole set on an agent builder.

/// Generate a stateless octo-code `Tool`. The workspace root is resolved before
/// the body runs and injected as `$root`; if resolution fails the tool returns
/// `{ "error": ... }` without invoking the body. The body evaluates to a
/// `serde_json::Value` (errors returned as data, never `Err`).
macro_rules! file_tool {
    (
        $(#[$meta:meta])*
        $tool:ident, $name:literal, $desc:expr, $args_ty:ty, $params:tt,
        |$root:ident, $args:ident| $body:expr
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Default)]
        pub struct $tool;

        impl ::rig::tool::Tool for $tool {
            const NAME: &'static str = $name;
            type Error = ::std::convert::Infallible;
            type Args = $args_ty;
            type Output = ::serde_json::Value;

            async fn definition(
                &self,
                _prompt: ::std::string::String,
            ) -> ::rig::completion::ToolDefinition {
                ::rig::completion::ToolDefinition {
                    name: $name.to_string(),
                    description: ($desc).to_string(),
                    parameters: ::serde_json::json!($params),
                }
            }

            async fn call(
                &self,
                $args: $args_ty,
            ) -> ::core::result::Result<::serde_json::Value, ::std::convert::Infallible> {
                let $root = match $crate::workspace::workspace_root() {
                    ::core::result::Result::Ok(r) => r,
                    ::core::result::Result::Err(e) => {
                        return ::core::result::Result::Ok(
                            ::serde_json::json!({ "error": e.to_string() }),
                        );
                    }
                };
                ::core::result::Result::Ok({ $body })
            }
        }
    };
}
pub(crate) use file_tool;

/// Register every octo-code file tool on a rig agent builder in one call:
///
/// ```ignore
/// let agent = octo_code::code_tools!(client.agent(model).preamble(p)).build();
/// ```
#[macro_export]
macro_rules! code_tools {
    ($builder:expr) => {
        $builder
            .tool($crate::ReadTool)
            .tool($crate::WriteTool)
            .tool($crate::EditTool)
            .tool($crate::ListTool)
            .tool($crate::GlobTool)
            .tool($crate::GrepTool)
    };
}
