# SimpleETL


INSTRUCTIONS:

There are three main query types:

    1.Getting the average or sum of one field of dataset
    2.Comparing a field of every row with a given number and list the results of the operation which is greater than or less than
    3.Search for a specific key : value

EXAMPLE USAGE FOR COMPARE

 	-http://localhost:4444/search/geomsource-Photogramm
 	-http://localhost:4444/compare/bin-lt-2000000

EXAMPLE USAGE FOR SEARCH

 	-http://localhost:4444/compare/cnstrctyr-gt-2015
 	-http://localhost:4444/search/heightroof-76.93
 	-http://localhost:4444/search/cnstrctyr-2013


EXAMPLE USAGE FOR GETTING AVERAGE AND SUM OF VALUES


 	-http://localhost:4444/op/avg-bin
 	-http://localhost:4444/op/sum-cnstrctyr


Do not use underscore while making a query:


    Fields:
        base_bbl
        bin
        cnstrct_yr
        doitt_id
        feat_code
        geomsource
        groundelev
        heightroof
        lstmoddate
        lststatype
        mpluto_bbl
        shape_area