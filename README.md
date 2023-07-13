# Stuck MDS fixer

ATTENTION: This is a work in progress. It is not ready for production yet. Furthermore this code performs a destructive operation on your filesystem. Use at your own risk.

## What is this?

This code is a fixer for hanging pods on Kubernetes because of ceph client client locking MDS indefinitely.

## How does it work?

The code:

- retrieves all the "operations in flight" from the ceph cluster (ceph fs mds)
- if some operations are waiting on a lock, it will retrieve the information of the ceph client session holding the lock
- Once it has retrieved the information about the kubernetes node and path, it tries to determine the uuid of the pod
- From there it connect to Kubernetes to retrieve the pod name and namespace
- Then it deletes the pod
- As a final step, it unmount the path from the node

## How to use it?

This code was created with Python 3.11.1. It should work with any Python 3.6+.

First, you need to clone the repository:

```bash
git clone git@github.com:jaepetto/ceph.stuck_mds_fixer.git
```

As with any python application, you need to install the dependencies first:

```bash
cd ceph.stuck_mds_fixer

python -m venv venv
pip install -r requirements.txt
```

Then you can run the code:

```bash
cd src
python main.py
```
