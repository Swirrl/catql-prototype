# Integration Testing

The datahost-ld-openapi project's integration testing is done mainy with [Hurl][hurl].

A test suite is defined by a directory structured according to certain [conventions](#conventions). The test suite is executed by the `bin/hurl-runner.bb` script which takes care of detecting any setup file and executing them before the test.

## Conventions

The contents of the directory looks close to this:

```
- hurl-scripts                       <-- --file root
	- common                           <-- common setup, data
		- users-appends-1.csv
		- user-appends-2.csv
		- schema-name-age.json
		- setup-revision.hurl
		- setup-revision-appeend.hurl
	- issue-99.hurl                    <-- test, no extra files
	- issue-101                        <-- test with extra files
		- setup.hurl                     <-- test setup
		- data1.csv                      <-- test data
		- data2.csv
		- issue-101.hurl                 <-- actual test script
	- issue-108.hurl
	- pr-85                            <-- test case
		- corrections.csv
		- pr-85.hurl                    <-- test script
		- setup.ref                     <-- setup script reference
	...
```

The scripts are run with the `--file-root` option set to this directory. The naming is important, but pretty easy to remember: if the test case can be contained in a single file, it ends up in an `pr-$NUM.hurl`, `issue-$NUM.hurl`, or `int-$NUM.hurl` file, otherwise it goes into a subdirectory `pr-$NUM`,`issue-$NUM`, or `int-$NUM` along with `setup.hurl` and any (optional) data files. 

Instead of a custom setup script, we can also use a setup reference: file named `setup.ref` which contents of is  set to path of a setup script to use (relative to the `hurl-scripts` directory, for example `common/setup-revision.hurl`).

There is also a `common` subdirectory which contains the most often used setup scripts and data. They are meant to be reused by different test cases.

If the test case requires specific setup, it can be placed in the test case's associated folder in a script named `setup.hurl`. Otherwise the test script can refer to a common library of scripts in the `common` directory.

All scripts receive the following variables:

- `scheme`: http | https
- `host_name`
- `series` random 
- `release` random
- auth_token (most likely passed via `HURL_auth_token` environment variable)

Additionally, it's possible to pass the below to ensure a service we want to deploy has the correct settings.

- `expected_uri_root` as host[:port]
- `expected_scheme`


[hurl]:https://hurl.dev