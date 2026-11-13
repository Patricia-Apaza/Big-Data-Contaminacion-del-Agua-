# Historial del USGS

Aquí va `historial.jsonl`, que no se versiona: pesa unos 430 MB y está en `.gitignore`.

Se genera desde `../patricia/` con el productor:

```
py usgs_productor.py historial --estaciones USGS-01491000,USGS-01579550,USGS-01589290,USGS-01594441,USGS-01640152,USGS-0164578734,USGS-0164579522,USGS-01646500,USGS-01649190,USGS-01649500,USGS-01650800,USGS-02054550,USGS-02074556 --dias 90 --variables temperatura_c,oxigeno_mgl,ph,turbidez_fnu --salida-archivo ..\data\historial.jsonl
```

Lo usan los cuadernos `12b` y `12e` (dentro del contenedor: `/opt/AquaVigia_Producto_U2/data/historial.jsonl`) y el productor en modo `reproducir`. Detalle en `docs/instalacion.md`.
