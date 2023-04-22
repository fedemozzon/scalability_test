### Para correr script con libreria de python de nebula

1. Crear ambiente virtual: `python3 -m venv venv`
2. Activar: `source venv/bin/activate`
3. Instalar: `pip install -r requirements.txt`
4. Correr archivo: `python test_x.py`
5. Siempre recordar hacer el paso 2 antes de correrlo

### Warning
La librería de Memgraph (gqlalchemy) tiene incompatibilidades con la de Neo4j. Por lo que si se quiere correr el script de Memgraph, se debe comentar la de Neo4j y viceversa.