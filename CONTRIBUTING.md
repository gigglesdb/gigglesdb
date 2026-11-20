# Contributing

Thanks for helping improve GigglesDB.

## Building and testing

For build prerequisites and test commands, see
[README.md: Development](README.md#development) and the per-crate READMEs
linked there. Each crate has its own manifest, so run commands with
`--manifest-path crates/<crate>/Cargo.toml` or from the crate's directory.

AI coding agents working in this repository follow [AGENTS.md](AGENTS.md).

## License of contributions

GigglesDB is licensed under the [Mozilla Public License 2.0](LICENSE).
Contributions are released under the same license. Existing third-party
components keep their own licenses; see
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

## Developer Certificate of Origin

Every commit must certify the Developer Certificate of Origin (DCO) below by
including a sign-off line with your real name and email:

```text
Signed-off-by: Your Name <you@example.com>
```

`git commit -s` adds it for you. By signing off, you confirm you have the
right to submit the change under the project's license. There is nothing else
to sign.

Verbatim from <https://developercertificate.org/>:

```text
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

## Code you did not write

Only submit work you own, or that you have the right to contribute. If a pull
request includes code, data or other material written by someone else:

- say so in the pull request description;
- name its origin (project, URL, version) and its license;
- keep its original copyright and license notices;
- list it in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

The material must use a license compatible with MPL-2.0. We may decline
material whose origin or license is unclear.

## Reporting security issues

Do not open public issues for vulnerabilities. Follow [SECURITY.md](SECURITY.md).

## CI and container releases

[CI and releases](docs/ci-releases.md) describes the test matrix, GHCR images,
configuration mounts and tag-based promotion. The remote landing gate remains
required; GitHub Actions does not replace it.
