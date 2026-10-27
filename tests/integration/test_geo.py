"""Geographic data, uploaded as GeoJSON."""

import pytest
from shapely.geometry import shape

from redivis import exceptions

from . import helpers

geopandas = pytest.importorskip("geopandas")


def test_geography_variable(geo_table):
    assert {v.name: v["type"] for v in geo_table.list_variables()} == {
        "geometry": "geography",
        "name": "string",
        "value": "integer",
    }


def test_to_geopandas_dataframe(geo_table):
    gdf = geo_table.to_geopandas_dataframe(progress=False)

    assert isinstance(gdf, geopandas.GeoDataFrame)
    assert gdf.crs == "EPSG:4326"
    assert gdf.geometry.name == "geometry"
    by_name = gdf.set_index("name")
    for feature in helpers.GEO_FEATURES:
        geometry = by_name.loc[feature["name"], "geometry"]
        assert by_name.loc[feature["name"], "value"] == feature["value"]
        # Edges come back with vertices added along them, which keep a geodesic
        # representation on the same (planar) path. So the shape is the same, though
        # its coordinates aren't.
        assert geometry.geom_type == feature["geometry"]["type"]
        assert geometry.hausdorff_distance(shape(feature["geometry"])) < 1e-9


def test_geography_variable_can_be_chosen_or_skipped(geo_table):
    gdf = geo_table.to_geopandas_dataframe(geography_variable="GEOMETRY", progress=False)
    assert gdf.geometry.name == "geometry"

    df = geo_table.to_geopandas_dataframe(geography_variable=None, progress=False)
    assert not isinstance(df, geopandas.GeoDataFrame)
    assert all(isinstance(value, str) for value in df["geometry"])

    with pytest.raises(exceptions.NotFoundError):
        geo_table.to_geopandas_dataframe(geography_variable="name_typo", progress=False)


def test_table_without_geography(tiny_table):
    with pytest.raises(exceptions.NotFoundError, match="geography"):
        tiny_table.to_geopandas_dataframe(progress=False)
