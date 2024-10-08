Commands used to create this:

```
pixi init
pixi task add test_main python test_io.py
pixi add intake numpy pandas # 1st failed run
pixi add s3fs  # 2nd failed run
pixi add jinja2  # 3rd failed run
pixi add zarr intake-xarray  # passed
# You can run with
pixi run test_main
```
