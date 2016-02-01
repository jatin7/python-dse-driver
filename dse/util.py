from itertools import chain


class Point(object):
    """
    Represents a point geometry for DSE
    """

    x = None
    """
    x coordinate of the point
    """

    y = None
    """
    y coordinate of the point
    """

    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __eq__(self, other):
        return self.x == other.x and self.y == other.y

    def __hash__(self):
        return hash((self.x, self.y))

    def __str__(self):
        """
        Well-known text representation of the point
        """
        return "POINT (%r %r)" % (self.x, self.y)

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self.x, self.y)


class Circle(object):
    """
    Represents a circle geometry for DSE
    This is not an official OGC type, but a DSE type.
    """

    x = None
    """
    x coordinate of the circle center
    """

    y = None
    """
    y coordinate of the circle center
    """

    r = None
    """
    radius of the circle
    """

    def __init__(self, x, y, r):
        self.x = x
        self.y = y
        self.r = r

    def __eq__(self, other):
        return self.x == other.x and self.y == other.y and self.r == other.r

    def __hash__(self):
        return hash((self.x, self.y, self.r))

    def __str__(self):
        """
        Well-known-text-like representation of the circle.
        (not an official OGC type)
        """
        return "CIRCLE ((%r %r) %r)" % (self.x, self.y, self.r)

    def __repr__(self):
        return "%s(%r, %r, %r)" % (self.__class__.__name__, self.x, self.y, self.r)


class LineString(object):
    """
    Represents a linestring geometry for DSE
    """

    coords = None
    """
    Tuple of (x, y) coordinates in the linestring
    """
    def __init__(self, coords):
        """
        'coords`: a sequence of (x, y) coordinates of points in the linestring
        """
        self.coords = tuple(coords)

    def __eq__(self, other):
        return self.coords == other.coords

    def __hash__(self):
        return hash(self.coords)

    def __str__(self):
        """
        Well-known text representation of the LineString
        """
        return "LINESTRING (%s)" % ', '.join("%r %r" % (x, y) for x, y in self.coords)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.coords)


class _LinearRing(object):
    # no validation, no implicit closing; just used for poly composition, to
    # mimic that of shapely.geometry.Polygon
    def __init__(self, coords):
        self.coords = tuple(coords)

    def __eq__(self, other):
        return self.coords == other.coords

    def __hash__(self):
        return hash(self.coords)

    def __str__(self):
        return "LINEARRING (%s)" % ', '.join("%r %r" % (x, y) for x, y in self.coords)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.coords)


class Polygon(object):
    """
    Represents a polygon geometry for DSE
    """

    exterior = None
    """
    _LinearRing representing the exterior of the polygon
    """

    interiors = None
    """
    Tuple of _LinearRings representing interior holes in the polygon
    """

    def __init__(self, exterior, interiors=None):
        """
        'exterior`: a sequence of (x, y) coordinates of points in the linestring
        `interiors`: None, or a sequence of sequences or (x, y) coordinates of points describing interior linear rings
        """
        self.exterior = _LinearRing(exterior)
        self.interiors = tuple(_LinearRing(e) for e in interiors) if interiors else tuple()

    def __eq__(self, other):
        return self.exterior == other.exterior and self.interiors == other.interiors

    def __hash__(self):
        return hash((self.exterior, self.interiors))

    def __str__(self):
        """
        Well-known text representation of the polygon
        """
        rings = (ring.coords for ring in chain((self.exterior,), self.interiors))
        rings = ("(%s)" % ', '.join("%r %r" % (x, y) for x, y in ring) for ring in rings)
        return "POLYGON (%s)" % ', '.join(rings)

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self.exterior.coords, [ring.coords for ring in self.interiors])