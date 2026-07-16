//! Outbound-URL hardening for client-supplied token-list URLs.
//!
//! `tokens_lists_urls` arrives from unauthenticated callers and is fetched
//! server-side, which makes it an SSRF vector: without a guard a caller can
//! point the service at cluster-internal endpoints (`*.svc.cluster.local`,
//! the kube API) or cloud metadata (`169.254.169.254`). Three layers close
//! the common bypasses:
//!
//! 1. [`validate_url`] — synchronous pre-flight: scheme must be http/https,
//!    a host must be present, and (when private hosts are disallowed) an
//!    IP-literal host must be publicly routable. Gives the caller a clean
//!    400 before any socket is opened.
//! 2. [`PublicOnlyDnsResolver`] — plugged into the reqwest client so every
//!    hostname the client ever connects to (including redirect targets and
//!    DNS-rebinding tricks) resolves through a filter that drops non-public
//!    addresses.
//! 3. [`redirect_policy`] — caps redirect hops and refuses redirects whose
//!    target host is a non-public IP literal (literals bypass DNS, so the
//!    resolver alone would not see them).
//!
//! The `allow_private_hosts` escape hatch exists for local development and
//! the integration suite, where token lists are served from `127.0.0.1`.

use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use reqwest::{redirect, Url};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Hard cap on redirect hops for token-list fetches.
const MAX_REDIRECT_HOPS: usize = 5;

/// Pre-flight validation of a caller-supplied token-list URL. Cheap and
/// synchronous — DNS-name hosts are *not* resolved here; they are enforced
/// at connect time by [`PublicOnlyDnsResolver`].
pub fn validate_url(raw_url: &str, allow_private_hosts: bool) -> Result<(), String> {
    let url = Url::parse(raw_url).map_err(|err| format!("invalid url: {err}"))?;

    match url.scheme() {
        "http" | "https" => {}
        other => return Err(format!("scheme '{other}' is not allowed, use http(s)")),
    }

    let Some(host) = url.host_str() else {
        return Err("url has no host".to_string());
    };

    if !allow_private_hosts {
        if let Some(ip) = ip_literal(host) {
            if !is_public_ip(ip) {
                return Err(format!("host {host} is not a public address"));
            }
        }
    }

    Ok(())
}

/// Parse a URL host as an IP literal. `Url::host_str` keeps the brackets
/// around IPv6 literals, so strip them before parsing.
fn ip_literal(host: &str) -> Option<IpAddr> {
    host.trim_start_matches('[')
        .trim_end_matches(']')
        .parse()
        .ok()
}

/// `true` iff `ip` is publicly routable — i.e. not loopback, RFC1918,
/// link-local (cloud metadata lives at `169.254.169.254`), CGNAT,
/// benchmarking, documentation, multicast, or reserved space.
pub fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_public_ipv4(v4),
        IpAddr::V6(v6) => is_public_ipv6(v6),
    }
}

fn is_public_ipv4(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_multicast()
        || octets[0] == 0 // 0.0.0.0/8 "this network"
        || (octets[0] == 100 && (octets[1] & 0xc0) == 64) // 100.64.0.0/10 CGNAT
        || (octets[0] == 192 && octets[1] == 0 && octets[2] == 0) // 192.0.0.0/24 IETF
        || (octets[0] == 198 && (octets[1] & 0xfe) == 18) // 198.18.0.0/15 benchmarking
        || octets[0] >= 240) // 240.0.0.0/4 reserved
}

fn is_public_ipv6(ip: Ipv6Addr) -> bool {
    // An IPv4-mapped address routes as its embedded v4 — classify that.
    if let Some(v4) = ip.to_ipv4_mapped() {
        return is_public_ipv4(v4);
    }

    let segments = ip.segments();
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_multicast()
        || (segments[0] & 0xfe00) == 0xfc00 // fc00::/7 unique local
        || (segments[0] & 0xffc0) == 0xfe80 // fe80::/10 link local
        || (segments[0] == 0x2001 && segments[1] == 0xdb8)) // 2001:db8::/32 documentation
}

/// DNS resolver that drops non-public addresses from every resolution. A
/// hostname resolving *only* to private space fails to resolve at all, so
/// the fetch errors out instead of reaching into the internal network —
/// this also covers DNS rebinding and redirects to attacker-controlled
/// names, which the pre-flight check cannot see.
pub struct PublicOnlyDnsResolver;

impl Resolve for PublicOnlyDnsResolver {
    fn resolve(&self, name: Name) -> Resolving {
        Box::pin(async move {
            let host = name.as_str().to_owned();
            let resolved = tokio::net::lookup_host((host.clone(), 0)).await?;
            let public: Vec<std::net::SocketAddr> =
                resolved.filter(|addr| is_public_ip(addr.ip())).collect();

            if public.is_empty() {
                let err: Box<dyn std::error::Error + Send + Sync> =
                    format!("host {host} does not resolve to any public address").into();
                return Err(err);
            }

            Ok(Box::new(public.into_iter()) as Addrs)
        })
    }
}

/// Redirect policy for the token-list client: bounded hop count, and (when
/// private hosts are disallowed) redirects to non-public IP literals are
/// refused. Redirects to hostnames are safe to follow — the connection goes
/// through [`PublicOnlyDnsResolver`] anyway.
pub fn redirect_policy(allow_private_hosts: bool) -> redirect::Policy {
    redirect::Policy::custom(move |attempt| {
        if attempt.previous().len() > MAX_REDIRECT_HOPS {
            return attempt.error("too many redirects");
        }

        if !allow_private_hosts {
            if let Some(host) = attempt.url().host_str() {
                if let Some(ip) = ip_literal(host) {
                    if !is_public_ip(ip) {
                        return attempt.error("redirect to a non-public address is not allowed");
                    }
                }
            }
        }

        attempt.follow()
    })
}

#[cfg(test)]
mod url_guard_tests {
    use super::*;

    #[test]
    fn rejects_non_http_schemes() {
        assert!(validate_url("ftp://example.com/list.json", false).is_err());
        assert!(validate_url("file:///etc/passwd", false).is_err());
        assert!(validate_url("gopher://example.com/x", false).is_err());
    }

    #[test]
    fn rejects_private_ip_literals() {
        for url in [
            "http://127.0.0.1/list.json",
            "http://10.0.0.1/list.json",
            "http://172.16.5.4/list.json",
            "http://192.168.1.1/list.json",
            "http://169.254.169.254/latest/meta-data",
            "http://100.64.0.1/list.json",
            "http://0.0.0.0/list.json",
            "http://[::1]/list.json",
            "http://[fc00::1]/list.json",
            "http://[fe80::1]/list.json",
            "http://[::ffff:10.0.0.1]/list.json",
        ] {
            assert!(
                validate_url(url, false).is_err(),
                "{url} should be rejected"
            );
        }
    }

    #[test]
    fn accepts_public_hosts() {
        assert!(validate_url("https://tokens.coingecko.com/uniswap/all.json", false).is_ok());
        assert!(validate_url("http://1.1.1.1/list.json", false).is_ok());
        // DNS names are validated at resolve time, not pre-flight.
        assert!(validate_url("https://example.com/list.json", false).is_ok());
    }

    #[test]
    fn allow_private_hosts_disables_ip_checks_but_not_scheme_checks() {
        assert!(validate_url("http://127.0.0.1:8080/list.json", true).is_ok());
        assert!(validate_url("ftp://127.0.0.1/list.json", true).is_err());
    }
}
