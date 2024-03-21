# Rapport BDA_Labo1_rPlaces
Auteurs: Machado Jorge, Acevedo Valentin

## Read-in r/places Data

For performance reasons and because our machines could only handle so much, we stuck to the lightest version of the dataset (500x500).


## Find the last paint event before the whitening
Here is the returned value by the *lastColorEvent* method:
```
2022-04-05T00:14:00.172Z
```


## Compute a ranking of colors
### Rank colors attempt #1:

Processing naive ranking took **8079 ms** and returned the following output:

```
List(
    (27,1616699), 
    (31,1461618), 
    (2,706722), 
    (12,578426), 
    (4,431632), 
    (13,253347), 
    (3,237898), 
    (18,225761), 
    (14,218502), 
    (8,180505), 
    (6,146267), 
    (1,140549), 
    (29,139909), 
    (23,132582), 
    (25,131534), 
    (30,119143), 
    (19,76291), 
    (24,68159), 
    (26,58186), 
    (7,53937), 
    (28,53001), 
    (22,49074), 
    (15,40300), 
    (9,32348), 
    (5,24705), 
    (17,23087), 
    (0,20461), 
    (21,19174), 
    (16,17895), 
    (20,11626), 
    (10,8137), 
    (11,6533)
)
```

The top 3 most used colours were:
1. Black (27)
2. White (31)
3. Dark Orange (2)

### Rank colors attempt #2:

Processing ranking using inverted index took **5823 ms** and returned the same as in the naive ranking method.
There is a significant performance improvement compared to the first method because the index is much quicker to go through than actual data records, which makes sense.


### Rank colors attempt #3:

Processing ranking using reduceByKey took **740 ms** and returned the same list as well. As we can see here, the performance gap is even bigger as the reduceByKey method allows distributed / parallel computation.


## Find for the n users who have placed the most pixels, their busiest tile
### Compute the n most active users

Processing most active users took **2465 ms** and returned the following output:

```
List(
    (85899866930,559), 
    (17180084138,443), 
    (68719685381,432), 
    (42950200594,429), 
    (25770217927,422), 
    (34360264713,420), 
    (103079423385,415), 
    (68719954231,412), 
    (51540074706,406), 
    (17180078961,403)
)
```

### Compute the busiest tile of the most active users

Processing Most active users busiest tile took **491 ms** and returned the following output:

```
List(
    (17180078961,(44,140)), 
    (68719685381,(34,279)), 
    (68719954231,(34,353)), 
    (42950200594,(43,255)), 
    (103079423385,(44,244)), 
    (85899866930,(44,547)), 
    (25770217927,(34,221)), 
    (51540074706,(44,238)), 
    (17180084138,(34,289)), 
    (34360264713,(44,196))
)
```
## Find the ratio of painting events at each coordinate which have the same color as just before the whitening
### Compute the colors at a given time

The *colorsAtTime* method returned the following result:

```
List(
    ((787,1154),31), 
    ((1163,1049),31), 
    ((1022,794),31), 
    ((814,1208),31), 
    ((967,1169),31), 
    ((900,1062),31), 
    ((999,950),31), 
    ((1219,840),31), 
    ((972,1102),31), 
    ((1107,780),31)
)
```

### Compute the ratio of painting events of each coordinate which have the color of the same coordinate at a given time

The *colorSimilarityBetweenHistoryAndGivenTime* method returned the following result:

```
List(
    ((759,817),1.0), 
    ((787,1104),1.0), 
    ((794,935),1.0), 
    ((803,992),1.0), 
    ((805,948),1.0), 
    ((816,973),1.0), 
    ((816,974),1.0), 
    ((817,1032),1.0), 
    ((817,1050),1.0), 
    ((818,1021),1.0)
)
```