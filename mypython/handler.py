from sqlalchemy.orm import sessionmaker
from models import Resource, db_connect, create_resource_table
import psycopg2
import json

def set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError


class ResourceHandler(object):
    """ Resource handler for res management """
    """
    Initialize database connection and sessionmaker.
    Creates resource table.
    """
    engine = db_connect()
    create_resource_table(engine)
    Session = sessionmaker(bind=engine)

    @classmethod
    def insert_item(self, **item):
        """ Save resource in the database.

        This is called for every item.

        """
        session = self.Session()
        resource = Resource(**item)

        try:
            session.add(resource)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
        return 'Success'


    @classmethod
    def get_all_items(self):
        session = self.Session()
        returnset = set() 
        for instance in session.query(Resource).all():
            returnset.add((instance.id, instance.name, instance.original_price, instance.status))
        return json.dumps(returnset, default=set_default)
