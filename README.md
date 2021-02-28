# Menmos - A Filesystem for the 2020s
Menmos is an open-source consumer-grade distributed blobstore and filesystem written in Rust. Its goal is to bring the main benefits of complex cloud-native filesystems into the mainstream. It is easy to deploy, easy to configure, and _blazing fast_, even on extremely modest systems.

![Version](https://img.shields.io/github/v/tag/menmos/menmos?label=version)
[![CI](https://github.com/menmos/menmos/actions/workflows/ci.yml/badge.svg)](https://github.com/menmos/menmos/actions/workflows/ci.yml)
![License](https://img.shields.io/github/license/menmos/menmos)

## Main Features
* 📁  Access files from all your computers as well as all your cloud storage under a single namespace - no need to remember on which machine you left your updated resume
* 📡  Streaming read and writes to all files - no complicated backup or sync processes
* 🔍 Powerful metadata and query functionality - don't spend anymore time coming up with the _perfect_ folder structure for your family photos
* 👪 Multi-user support - Securely share your storage space with your family
### More Neat Stuff
* Baked-in HTTPS with automatic certificate renewal (via [Let's Encrypt](https://letsencrypt.org/))
* FUSE support
* CLI client
* Shareable links _(planned)_
* Data replication rules _(planned)_
* Windows mount support (via Dokan) _(planned)_
* Web interface _(planned)_
* Native apps _(planned)_

## Disclaimer
Menmos is currently in _very early_ alpha. As such, it shouldn't be used as the main source of storage for your files or - _god forbid_ - in a production system.

Ignoring this warning _will_ result in data (and feelings) getting hurt.

That being said, if you have an apetite for danger and want to play with the technology to help us make menmos more stable for everyday use, then by all means read on!

## Architecture
A Menmos server system is composed of two components: `menmosd` and `amphora`.

### menmosd
menmosd is the master node of a Menmos cluster. Its responsabilities include keeping track of where files are stored, executing queries, keeping the system coherent, and being the central dispatch server for the cluster.

### amphora
The amphora, also known as the _Storage Node_ is a file server that integrates in a menmos cluster. An amphora transparently serves files either from disk or from a cloud provider.

### File Upload
To upload a file to the Menmos cluster, the client first sends an upload request to `menmosd`. The master node inspects the metadata of the uploaded file and redirects the client to the amphora that will store the file. When the upload is complete, the amphora returns the file ID to the client.

### File Recovery
To read or write to a file, the process is similar. The client sends a read/write request to `menmosd` and is redirected to the appropriate storage node for the actual operation.

### Network Efficiency
Menmos tries to be efficient in how it uses the network. As such, your data is _never_ routed through the master node. Furthermore, the system will attempt to use the local network when possible to make read and write operations as fast as they can be.

## Setting Up
Both `menmosd` and `amphora` can be deployed directly from the binary, or from a docker image.
Since both processes are similar, we'll only cover the native binary in this README.

### Deploying `menmosd`
Starting a menmosd instance is very simple:
```bash
$ menmosd --cfg config.toml
```
__Note:__ When running menmosd in HTTPS mode, the server will most likely need elevated privileges in order to bind to port 53.

#### menmosd Sample Configuration - HTTP
```toml
# config_http.toml

[server]
type = "HTTP"
port = 3030

[node]
admin_password = "<password>" # The password of the cluster admin user.
encryption_key = "<encryption key>" # The encryption key *must* be exactly 32 characters long.

```
#### menmosd Sample Configuration - HTTPS
```toml
# config_https.toml

# The HTTPS server listens by default on ports 53 (for DNS) and 443.
[server]
type = "HTTPS"
letsencrypt_email = "hello@menmos.xyz" # The email to use for you letsencrypt account.
certificate_storage_path = "./certificates" # The directory in which to store your certificates.

# In HTTPS mode, menmosd embeds a DNS server for resolving the various nodes.
# The server running menmosd should be configured as the authoritative name server for
# the domain you set in the `root_domain` key.
[server.dns]
host_name = "directory.menmos.you.com" # This is the the desired host name
root_domain = "menmos.you.com" # The domain under which all nodes will be assigned.
public_ip = "100.110.103.52" # The public IP of the server running menmosd

[node]
admin_password = "<password>" # The password of the cluster admin user.
encryption_key = "<encryption key>" # The encryption key *must* be exactly 32 characters long.
```

### Deploying `amphora`
Starting an amphora is just as simple:
```bash
$ amphora --cfg config.toml
```
#### amphora Sample Configuration
```toml
# The URL and port of the menmosd instance you want this amphora to join
[directory]
url = "https://directory.menmos.you.com" # The scheme (http v. https) is significant here, be sure to specify it
port = 443

[node]
name = "alpha" # The name of this amphora - should be unique within a cluster
encryption_key = "<encryption key>" # Should match the key set on the menmosd server - it's the only way both servers can trust each other

# Specify the storage backend for this amphora.
# Currently, only "Directory" and "S3" are available.
[node.blob_storage]
type = "Directory"
path = "/tmp/blobs"

[server]
port = 3031 # The port this amphora will be listening on

```

### Network Assumptions
The only thing Menmos assumes about your network setup is that amphorae should be able to connect to the menmosd server. menmosd does not need to be able to connect to the amphorae, nor do the amphorae need to be able to connect to each other.

As mentioned in the setup section, if running in HTTPS mode, the menmosd server must be configured as the authoritative nameserver for the subdomain the cluster is running on.

### Security Considerations
Even though you can absolutely run a menmosd instance in the cloud along with a firewalled amphora that is not exposed to the internet, doing so might lead to some puzzling behavior for users of your cluster (e.g. some files disappearing from a directory when the user leaves the house because direct connection to the amphora is lost).

If you do not want to expose your amphora to the public internet, we strongly recommend running your cluster through something like [Tailscale](https://tailscale.com/) - it's what we do too :)

## Additional Documentation
[CLI Reference](https://github.com/menmos/menmos/wiki/CLI-Reference)

[Query Syntax](https://github.com/menmos/menmos/wiki/Query-Syntax)
