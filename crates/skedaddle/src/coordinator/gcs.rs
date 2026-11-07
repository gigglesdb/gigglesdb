//! Append-only GCS JSON API adapter. The pinned emulator does not support the
//! object_store 0.14.2 XML write path. Credentials still come from object_store.
//! See <https://cloud.google.com/storage/docs/json_api/v1/objects/insert>.

use futures::{
    StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
use object_store::{gcp::GoogleCloudStorage, path::Path, *};
use serde::Deserialize;
use std::{io, time::Duration};

#[derive(Clone)]
pub struct GcsJsonStore {
    bucket: String,
    endpoint: reqwest::Url,
    auth: GoogleCloudStorage,
    client: reqwest::Client,
    page_size: usize,
}
impl std::fmt::Debug for GcsJsonStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsJsonStore")
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Display for GcsJsonStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GCS JSON ({})", self.bucket)
    }
}
impl GcsJsonStore {
    pub fn new(bucket: impl Into<String>, auth: GoogleCloudStorage) -> Result<Self> {
        Self::build(bucket.into(), "https://storage.googleapis.com", auth, false)
    }
    /// HTTP is enabled only through this explicit emulator constructor.
    pub fn for_emulator(
        bucket: impl Into<String>,
        endpoint: &str,
        auth: GoogleCloudStorage,
    ) -> Result<Self> {
        Self::build(bucket.into(), endpoint, auth, true)
    }
    fn build(
        bucket: String,
        endpoint: &str,
        auth: GoogleCloudStorage,
        emulator: bool,
    ) -> Result<Self> {
        let endpoint = reqwest::Url::parse(endpoint).map_err(gcs_error)?;
        if bucket.is_empty()
            || bucket.contains('/')
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || !(endpoint.scheme() == "https" || (emulator && endpoint.scheme() == "http"))
        {
            return Err(gcs_invalid(
                "invalid GCS JSON bucket/endpoint; HTTPS required outside emulator mode",
            ));
        }
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(gcs_error)?;
        Ok(Self {
            bucket,
            endpoint,
            auth,
            client,
            page_size: 1000,
        })
    }
    pub fn with_list_page_size(mut self, size: usize) -> Result<Self> {
        if !(1..=1000).contains(&size) {
            return Err(gcs_invalid("GCS list page size must be 1..=1000"));
        }
        self.page_size = size;
        Ok(self)
    }
    fn url(&self, upload: bool, location: Option<&Path>) -> Result<reqwest::Url> {
        let mut url = self.endpoint.clone();
        let mut parts = url
            .path_segments_mut()
            .map_err(|_| gcs_invalid("GCS endpoint cannot be a base URL"))?;
        parts.pop_if_empty();
        if upload {
            parts.push("upload");
        }
        parts.extend(["storage", "v1", "b", &self.bucket, "o"]);
        if let Some(location) = location {
            parts.push(location.as_ref());
        }
        drop(parts);
        Ok(url)
    }
    async fn request(
        &self,
        method: reqwest::Method,
        url: reqwest::Url,
    ) -> Result<reqwest::RequestBuilder> {
        let credential = self.auth.credentials().get_credential().await?;
        let request = self.client.request(method, url);
        Ok(if credential.bearer.is_empty() {
            request
        } else {
            request.bearer_auth(&credential.bearer)
        })
    }
    async fn checked(&self, response: reqwest::Response, path: &Path) -> Result<reqwest::Response> {
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let body = response.text().await.map_err(gcs_error)?;
        Err(gcs_status_error(status.as_u16(), path, body))
    }
    fn unsupported(&self, operation: &str) -> Error {
        Error::NotImplemented {
            operation: operation.into(),
            implementer: self.to_string(),
        }
    }
    async fn list_page(
        &self,
        prefix: &str,
        token: Option<&str>,
    ) -> Result<(Vec<ObjectMeta>, Option<String>)> {
        let mut url = self.url(false, None)?;
        url.query_pairs_mut()
            .append_pair("prefix", prefix)
            .append_pair("maxResults", &self.page_size.to_string());
        if let Some(token) = token {
            url.query_pairs_mut().append_pair("pageToken", token);
        }
        let response = self
            .request(reqwest::Method::GET, url)
            .await?
            .send()
            .await
            .map_err(gcs_error)?;
        let response = self.checked(response, &Path::from(prefix)).await?;
        let page: GcsPage = response.json().await.map_err(gcs_error)?;
        if page.next_page_token.as_deref() == token && token.is_some() {
            return Err(gcs_invalid("GCS pagination token did not advance"));
        }
        Ok((
            page.items
                .into_iter()
                .map(gcs_metadata)
                .collect::<Result<Vec<_>>>()?,
            page.next_page_token,
        ))
    }
}

#[async_trait::async_trait]
impl ObjectStore for GcsJsonStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        if !matches!(options.mode, PutMode::Create)
            || !options.attributes.is_empty()
            || !options.tags.is_empty()
        {
            return Err(
                self.unsupported("append-only JSON put supports Create without attributes/tags")
            );
        }
        let mut url = self.url(true, None)?;
        url.query_pairs_mut()
            .append_pair("uploadType", "media")
            .append_pair("name", location.as_ref())
            .append_pair("ifGenerationMatch", "0");
        let bytes = payload
            .iter()
            .flat_map(|chunk| chunk.iter().copied())
            .collect::<Vec<_>>();
        let response = self
            .request(reqwest::Method::POST, url)
            .await?
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(bytes)
            .send()
            .await
            .map_err(gcs_error)?;
        let response = self.checked(response, location).await?;
        let meta = gcs_metadata(response.json().await.map_err(gcs_error)?)?;
        Ok(PutResult {
            e_tag: meta.e_tag,
            version: meta.version,
            extensions: Default::default(),
        })
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let mut url = self.url(false, Some(location))?;
        if let Some(version) = &options.version {
            url.query_pairs_mut().append_pair("generation", version);
        }
        let response = self
            .request(reqwest::Method::GET, url.clone())
            .await?
            .send()
            .await
            .map_err(gcs_error)?;
        let response = self.checked(response, location).await?;
        let meta = gcs_metadata(response.json().await.map_err(gcs_error)?)?;
        options.check_preconditions(&meta)?;
        let range = match &options.range {
            Some(range) => range.as_range(meta.size).map_err(gcs_error)?,
            None => 0..meta.size,
        };
        let data = if options.head {
            Vec::new().into()
        } else {
            url.query_pairs_mut().append_pair("alt", "media");
            if options.version.is_none()
                && let Some(version) = &meta.version
            {
                url.query_pairs_mut().append_pair("generation", version);
            }
            let mut request = self.request(reqwest::Method::GET, url).await?;
            if options.range.is_some() {
                request = request.header(
                    reqwest::header::RANGE,
                    format!("bytes={}-{}", range.start, range.end - 1),
                );
            }
            let response = self
                .checked(request.send().await.map_err(gcs_error)?, location)
                .await?;
            let data = response.bytes().await.map_err(gcs_error)?;
            if data.len() as u64 != range.end - range.start {
                return Err(gcs_invalid(
                    "GCS object read was truncated or exceeded its requested range",
                ));
            }
            data
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(data) }).boxed()),
            meta,
            range,
            attributes: Default::default(),
            extensions: Default::default(),
        })
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = prefix.map_or_else(String::new, |prefix| {
            format!("{}/", prefix.as_ref().trim_end_matches('/'))
        });
        stream::try_unfold(
            (self.clone(), prefix, None::<String>, false),
            |(store, prefix, token, done)| async move {
                if done {
                    return Ok::<_, Error>(None);
                }
                let (items, next) = store.list_page(&prefix, token.as_deref()).await?;
                let done = next.is_none();
                Ok(Some((items, (store, prefix, next, done))))
            },
        )
        .map_ok(|items| stream::iter(items.into_iter().map(Ok)))
        .try_flatten()
        .boxed()
    }
    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        Err(self.unsupported("multipart upload"))
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let store = self.clone();
        locations
            .map(move |path| path.and_then(|_| Err(store.unsupported("delete"))))
            .boxed()
    }
    async fn list_with_delimiter(&self, _: Option<&Path>) -> Result<ListResult> {
        Err(self.unsupported("delimiter listing"))
    }
    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> Result<()> {
        Err(self.unsupported("copy"))
    }
    async fn rename_opts(&self, _: &Path, _: &Path, _: RenameOptions) -> Result<()> {
        Err(self.unsupported("rename"))
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum GcsNumber {
    String(String),
    Number(u64),
}
impl GcsNumber {
    fn number(self) -> Result<u64> {
        match self {
            Self::String(value) => value.parse().map_err(gcs_error),
            Self::Number(value) => Ok(value),
        }
    }
}
#[derive(Deserialize)]
struct GcsObject {
    name: String,
    generation: GcsNumber,
    size: GcsNumber,
    updated: String,
    etag: Option<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GcsPage {
    #[serde(default)]
    items: Vec<GcsObject>,
    next_page_token: Option<String>,
}
fn gcs_metadata(object: GcsObject) -> Result<ObjectMeta> {
    Ok(ObjectMeta {
        location: Path::parse(object.name).map_err(gcs_error)?,
        size: object.size.number()?,
        last_modified: object.updated.parse().map_err(gcs_error)?,
        e_tag: object.etag,
        version: Some(object.generation.number()?.to_string()),
    })
}
fn gcs_error(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Generic {
        store: "GCS JSON",
        source: Box::new(error),
    }
}
fn gcs_invalid(message: &str) -> Error {
    gcs_error(io::Error::new(io::ErrorKind::InvalidData, message))
}
fn gcs_status_error(status: u16, path: &Path, body: String) -> Error {
    let source = Box::new(io::Error::other(format!("GCS JSON HTTP {status}: {body}")));
    match status {
        404 => Error::NotFound {
            path: path.to_string(),
            source,
        },
        412 => Error::AlreadyExists {
            path: path.to_string(),
            source,
        },
        401 | 403 => Error::PermissionDenied {
            path: path.to_string(),
            source,
        },
        _ => Error::Generic {
            store: "GCS JSON",
            source,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::gcp::GoogleCloudStorageBuilder;
    #[test]
    fn metadata_is_typed_and_rejects_malformed_fields() {
        let value = serde_json::json!({"name":"p2/key","generation":"42","size":"3","updated":"1970-01-01T00:00:00Z","etag":"tag"});
        let meta = gcs_metadata(serde_json::from_value(value.clone()).unwrap()).unwrap();
        assert_eq!(meta.location, Path::from("p2/key"));
        assert_eq!(meta.size, 3);
        assert_eq!(meta.version.as_deref(), Some("42"));
        assert_eq!(meta.e_tag.as_deref(), Some("tag"));
        assert_eq!(meta.last_modified.timestamp(), 0);
        for (field, bad) in [
            ("size", "bad"),
            ("generation", "-1"),
            ("updated", "not a date"),
        ] {
            let mut value = value.clone();
            value[field] = bad.into();
            assert!(gcs_metadata(serde_json::from_value(value).unwrap()).is_err());
        }
    }
    #[test]
    fn status_codes_preserve_failure_categories() {
        let path = Path::from("key");
        assert!(matches!(
            gcs_status_error(404, &path, "failed".into()),
            Error::NotFound { .. }
        ));
        assert!(matches!(
            gcs_status_error(412, &path, "failed".into()),
            Error::AlreadyExists { .. }
        ));
        for code in [401, 403] {
            assert!(matches!(
                gcs_status_error(code, &path, "failed".into()),
                Error::PermissionDenied { .. }
            ));
        }
        assert!(matches!(
            gcs_status_error(503, &path, "failed".into()),
            Error::Generic { .. }
        ));
    }
    #[tokio::test]
    async fn constructors_require_safe_endpoints_and_page_bounds() {
        let auth = GoogleCloudStorageBuilder::new()
            .with_bucket_name("bucket")
            .with_service_account_key(
                r#"{"private_key":"","private_key_id":"","client_email":"","disable_oauth":true}"#,
            )
            .with_skip_signature(true)
            .build()
            .unwrap();
        for (url, emulator) in [
            ("http://localhost", false),
            ("ftp://localhost", true),
            ("https://user:pw@localhost", true),
            ("https://user@localhost", true),
            ("https://:pw@localhost", true),
            ("https://localhost/?q=v", true),
            ("https://localhost/#fragment", true),
        ] {
            assert!(GcsJsonStore::build("bucket".into(), url, auth.clone(), emulator).is_err());
        }
        for bucket in ["", "a/b"] {
            assert!(GcsJsonStore::new(bucket, auth.clone()).is_err());
        }
        let store = GcsJsonStore::new("bucket", auth).unwrap();
        assert_eq!(store.to_string(), "GCS JSON (bucket)");
        assert!(format!("{store:?}").contains("bucket"));
        assert!(!format!("{store:?}").contains("private_key"));
        assert!(store.clone().with_list_page_size(0).is_err());
        assert!(store.clone().with_list_page_size(1001).is_err());
        store.clone().with_list_page_size(1).unwrap();
        store.with_list_page_size(1000).unwrap();
    }
}
