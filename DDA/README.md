Performs ***Dimensional Data Analysis*** on Baseball data.
See the DDA paper below.

The point is to classify entities (e.g. birthState, height, etc.)
by their (Ni,Vi,Mi) triples, where

- 	Ni = # unique rows
- 	Vi = # nonzero values
- 	Mi = # unique columns (in the exploded form)

This serves as a useful first step in understanding the structure of a database.  The classification may help in future analytics.

#####Quick Breakdown of Entity Types:

* Identity: Ni ~ Mi
* Authoritative: Ni << Mi
* Organizational: Ni >> Mi
* Vestigial: Ni, Mi ~ 1


### References

Gadepally, Vijay and Jeremy Kepner.  "Big Data Dimensional Analysis".  Submitted to IEEE HPEC 2014.
