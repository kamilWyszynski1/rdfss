use axum::extract::{MatchedPath, Request};
use axum::Router;
use dotenvy::dotenv;
use std::time::Duration;
use std::{env, thread};
use tower_http::trace::TraceLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{filter, fmt, Layer};

pub fn init_tracing() {
    let filter = filter::filter_fn(|metadata| {
        // Replace "your_crate_name" with the actual name of your crate
        metadata.target().starts_with("rdfss")
    });
    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .pretty()
                .with_filter(filter::LevelFilter::from(tracing::Level::DEBUG))
                .with_filter(filter),
        )
        .init();
}

pub fn inject_tracing_layer(r: Router) -> Router {
    r.layer(
        TraceLayer::new_for_http()
            // Create our own span for the request and include the matched path. The matched
            // path is useful for figuring out which handler the request was routed to.
            .make_span_with(|req: &Request| {
                let method = req.method();
                let uri = req.uri();

                // axum automatically adds this extension.
                let matched_path = req
                    .extensions()
                    .get::<MatchedPath>()
                    .map(|matched_path| matched_path.as_str());

                tracing::info_span!("request", %method, %uri, matched_path)
            })
            // By default, `TraceLayer` will log 5xx responses but we're doing our specific
            // logging of errors so disable that
            .on_failure(()),
    )
}
