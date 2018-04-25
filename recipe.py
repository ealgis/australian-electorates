#
# EAlGIS loader: Australian electorate boundaries
#

from ealgis_common.db import DataLoaderFactory
from ealgis_common.util import make_logger
from ealgis_common.loaders import ZipAccess, ShapeLoader, MapInfoLoader
import os.path


logger = make_logger(__name__)


def one(l):
    if len(l) == 0:
        raise Exception('one(): zero entries')
    elif len(l) > 1:
        raise Exception('one(): more than one entry (%s)' % repr(l))
    return l[0]


def load_shapes(factory, basedir, tmpdir):
    def load_mapinfo(table_name, filename):
        with ZipAccess(None, tmpdir, filename) as z:
            mipath = z.getdir() + '/'
            logger.debug([tmpdir, filename, mipath])
            instance = MapInfoLoader(loader.dbschema(), mipath, table_name=table_name)
            instance.load(loader)

    def load_shapefile(table_name, filename, srid):
        with ZipAccess(None, tmpdir, filename) as z:
            shpfile = one(z.glob('*.shp'))
            instance = ShapeLoader(loader.dbschema(), shpfile, srid, table_name=table_name)
            instance.load(loader)

    FED_DESCR = 'Australian Federal Electorate Boundaries as at the %d election'
    WGS84 = 4326
    GDA94 = 4283
    shapes = {
        ('au_federal_electorate_boundaries', 'Australian Federal Electorate Boundaries (AEC)'): [
            ('federal_2001', FED_DESCR % 2001, 'Federal/COM_ELB_2001.zip', load_mapinfo, ()),
            ('federal_2004', FED_DESCR % 2004, 'Federal/COM_ELB_2004.zip', load_shapefile, (WGS84,)),
            ('federal_2007', FED_DESCR % 2007, 'Federal/COM_ELB_2007.zip', load_shapefile, (WGS84,)),
            ('federal_2010', FED_DESCR % 2010, 'Federal/national-esri-2010.zip', load_shapefile, (GDA94,)),
            ('federal_2013', FED_DESCR % 2013, 'Federal/national-esri-16122011.zip', load_shapefile, (GDA94,)),
            ('federal_2016', FED_DESCR % 2016, 'Federal/national-midmif-09052016.zip', load_mapinfo, ())
        ],
        ('au_wa_state_electorate_boundaries', 'Western Australian State Electorate Boundaries (EBWA)'): [
            ('wa_2011_la', 'Western Australian Legislative Assembly 2011', 'WA/2011/waec2011_final_boundaries.zip', load_shapefile, (WGS84,)),
            ('wa_2011_lc', 'Western Australian Legislative Council 2011', 'WA/2011/waec2011_final_boundaries_lc.zip', load_shapefile, (WGS84,)),
            ('wa_2015_la', 'Western Australian Legislative Assembly 2015', 'WA/2015/2015 Final Boundaries LA.zip', load_mapinfo, ()),
            ('wa_2015_lc', 'Western Australian Legislative Council 2015', 'WA/2015/2015 Final Boundaries LC.zip', load_mapinfo, ()),
        ]
    }

    results = []
    for (schema_name, schema_description), to_load in shapes.items():
        loader = factory.make_loader(schema_name, mandatory_srids=[3112, 3857])
        loader.set_metadata(name='', description=schema_description)
        loader.session.commit()
        for table_name, description, zip_path, loader_fn, loader_args in to_load:
            loader_fn(table_name, os.path.join(basedir, zip_path), *loader_args)
            loader.set_table_metadata(table_name, {'description': description})
        results.append(loader.result())
    return results


def main():
    tmpdir = "/app/tmp"
    basedir = '/app/'
    factory = DataLoaderFactory("scratch", clean=False)
    shape_results = load_shapes(factory, basedir, tmpdir)
    for result in shape_results:
        result.dump(tmpdir)


if __name__ == '__main__':
    main()
