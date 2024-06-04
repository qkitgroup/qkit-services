# Ganymed Jupyter Lab Notebook Monitor and Reporter
Reports the last used juypter notebook to influx db.

Install with pip.
```bash
pip install -e .
```

Enable for jupyter server:
```bash
jupyter server extension enable --user --py ganymed
```

Configure in local Jupyter Lab Config.
```python
c = get_config() 
c.GanymedServer.influx_address = "Some-Host"
c.GanymedServer.influx_token = "Some-Token"
c.GanymedServer.influx_org = "Some-Org"
```