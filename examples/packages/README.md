# Sample flow packages

`acme-invoices` is a customer repository in the M1 project layout, with a `version:` in its `feather.yaml`, as
`feather package build` needs. The flow package tests build, publish and fetch it; later stages install and activate
it. Its `secrets/` folder is there to show that a build leaves it out.
