import base64
from graphrag.query.llm.oai.chat_openai import ChatOpenAI
from graphy.parser import DocumentChunk
import time
import json


ITERATIVE_ANALYSIS_CLASSIFIER_STEP = """Look at the provided image and classify it into a category + sub-category as described below:

Categories + Sub-categories: 

* table
    ** standard
    ** matrix
    ** pivot
    ** cross-tab
    ** nested
    ** other
* chart
    ** bar
    ** line
    ** pie
    ** scatter
    ** histogram
    ** box
    ** time-series
    ** heat-map
    ** network
    ** venn
    ** sankey
    ** tree
    ** radar
    ** bubble
    ** waterfall
    ** gantt
    ** other
* formula
    ** equation
    ** expression
    ** other
* text
    ** paragraph
    ** list
    ** title
    ** other
* picture
    ** diagram
    ** photo
    ** drawing
    ** other
* radiograph
    ** x-ray
    ** MRI
    ** CT
    ** other
* other
    ** other

Return the category and sub-category of the image in the following JSON format: 

{ "category": "category", "sub_category": "sub-category" }

eg. If the category is "table" and the sub-category is "standard", return:

{ "category": "table", "sub_category": "standard" }


Do not describe what you are doing, and do not use wordage like "the image contains ...", only return the exact JSON as described above.
"""

ITERATIVE_ANALYSIS_TABLE_RULES_STANDARD = """Analyse the image of the standard table provided and return the content of the table in markdown format.

Be very precise in determining the data in each cell, and include the table headers if they are present.

If a cell contains a formula, equation, or expression, include the formula or equation in the cell in LaTeX format.

Do not describe what you are doing, and do not use wordage like "the image contains ...", only return the table as markdown.

For simple, non-nested tables, use standard markdown table formatting, like this:
eg. 
```markdown
| HEADER 1 | HEADER 2 | HEADER 3 |
|----------|----------|----------|
| 0.56     | 1.72     | 0.13     |
```

If the table is more complex, with nested data, then use markdown with embedded latex to precisely describe the table data structure, like this: 

eg. 
```markdown
$$\begin{tabular}{|c|c|c|c|c|c|}
\hline
\textbf{GENDER} & \textbf{} & \textbf{AGE} & \textbf{} & \textbf{} & \textbf{} \
\cline{1-2} \cline{4-6}
MALE & FEMALE & $<$20 & 20-40 & 40-60 & 60+ \
\hline
56% & 44% & 12% & 24% & 32% & 32% \
\hline
\end{tabular}$$
```

"""


ITERATIVE_ANALYSIS_TABLE_RULES_MATRIX = """Analyse the image of the matrix table provided and return the content of the table in markdown format.

Be very precise in determining the data in each cell, and include the table headers if they are present.

For each cell 
  - If the background colour is important (eg. RED vs. GREEN), include the colour in the cell, along with data.
  - If there is a symbol or icon in the cell that indicates a state (Usually described by a legend), include the state (or icon) in the cell, along with data.
  - If the font size or style is important (eg. BOLD vs. ITALIC), include the font style in the cell, along with data.

eg. 

```markdown
| HEADER 1                   | HEADER 2                  | HEADER 3                  |
|----------------------------|---------------------------|---------------------------|
|  COLOR: RED, DATA: 0.56    | COLOR: GREEN, DATA: 1.72  |  COLOR: RED, DATA: 0.13   |
```
"""

ITERATIVE_ANALYSIS_TABLE_RULES_PIVOT = """Analyse the image of the pivot table provided and return the content of the table in markdown format.

Be very precise in determining the data in each cell, and include the table headers if they are present.

For each cell
    - If the cell contains a formula, equation, or expression, include the formula or equation in the cell in LaTeX format.
    - If the cell contains a symbol or icon that indicates a state (Usually described by a legend), include the state (or icon) in the cell, along with data.
    - If the cell contains a hyperlink, include the hyperlink in the cell, along with data.

eg.

```markdown
| HEADER 1 | HEADER 2 | HEADER 3 |
|----------|----------|----------|
| 0.56     | 1.72     | 0.13     |
```
"""

ITERATIVE_ANALYSIS_TABLE_RULES_CROSSTAB = """Analyse the image of the cross-tab table provided and return the content of the table in markdown format.

Be very precise in determining the data in each cell, and include the table headers if they are present.

For each cell
    - If the cell contains a formula, equation, or expression, include the formula or equation in the cell in LaTeX format.
    - If the cell contains a symbol or icon that indicates a state (Usually described by a legend), include the state (or icon) in the cell, along with data.
    - If the cell contains a hyperlink, include the hyperlink in the cell, along with data.

Given this type of table is usually nested data (like Gender, with Male + Female sub-categories, and Age with sub-categories like <20, 20-40, 40-60, 60+), use markdown with embedded latex to precisely describe the table data structure.
eg. The following table describes the percentage of students that are Male and Female, and their age distribution:

```markdown
$$\begin{tabular}{|c|c|c|c|c|c|}
\hline
\textbf{GENDER} & \textbf{} & \textbf{AGE} & \textbf{} & \textbf{} & \textbf{} \
\cline{1-2} \cline{4-6}
MALE & FEMALE & $<$20 & 20-40 & 40-60 & 60+ \
\hline
56% & 44% & 12% & 24% & 32% & 32% \
\hline
\end{tabular}$$
```
"""

ITERATIVE_ANALYSIS_TABLE_RULES_NESTED = """Analyse the image of the nested table provided and return the content of the table in markdown format.

Be very precise in determining the data in each cell, and include the table headers if they are present.

For each cell
    - If the cell contains a formula, equation, or expression, include the formula or equation in the cell in LaTeX format.
    - If the cell contains a symbol or icon that indicates a state (Usually described by a legend), include the state (or icon) in the cell, along with data.
    - If the cell contains a hyperlink, include the hyperlink in the cell, along with data.

Given this type of table is usually nested data, use markdown with embedded latex to precisely describe the table data structure.

eg. The following table describes the percentage of students that are Male + Female, and their age distribution:

```markdown
$$\begin{tabular}{|c|c|c|c|c|c|}
\hline
\textbf{GENDER} & \textbf{} & \textbf{AGE} & \textbf{} & \textbf{} & \textbf{} \
\cline{1-2} \cline{4-6}
MALE & FEMALE & $<$20 & 20-40 & 40-60 & 60+ \
\hline
56% & 44% & 12% & 24% & 32% & 32% \
\hline
\end{tabular}$$
```
"""

ITERATIVE_ANALYSIS_TABLE_RULES_OTHER = ITERATIVE_ANALYSIS_TABLE_RULES_STANDARD

ITERATIVE_ANALYSIS_CHART_RULES_BAR = """Analyse the image of the bar chart provided and precisely describe what the graph is showing.

Include the data from the legend (if available), the axes of the graph, and an interpretation of the data series.

eg. The following is an example description of a bar chart for sales of data for the first quarter of 2022 in 3 product categories (A, B, C):

The following bar chart shows the sales data for the first quarter of 2022, with the x-axis representing the months (Jan, Feb, Mar) and the y-axis representing the sales amount in USD. 
The data series shows the sales amount for each product category (A, B, C).
Product category A has the highest sales in January, followed by category B in February, and category C in March.

Product Category A: Jan: $100,000, Feb: $80,000, Mar: $60,000
Product Category B: Jan: $80,000, Feb: $60,000, Mar: $40,000
Product Category C: Jan: $60,000, Feb: $40,000, Mar: $20,000

"""

ITERATIVE_ANALYSIS_CHART_RULES_LINE = """Analyse the image of the line chart provided and precisely describe what the graph is showing.

Include the data from the legend (if available), the axes of the graph, and an interpretation of the data series.

eg. The following is an example description of a line chart with two series (A, B) showing the temperature in Celsius over a 24-hour period:

The line chart shows the temperature in Celsius over a 24-hour period, with the x-axis representing the time of day and the y-axis representing the temperature in Celsius.
The data series shows the temperature for two locations (A, B) over the 24-hour period.

Location A: 12:00 AM: 20°C, 6:00 AM: 25°C, 12:00 PM: 30°C, 6:00 PM: 35°C
Location B: 12:00 AM: 18°C, 6:00 AM: 23°C, 12:00 PM: 28°C, 6:00 PM: 33°C

"""

ITERATIVE_ANALYSIS_CHART_RULES_PIE = """Analyse the image of the pie chart provided and precisely describe what the graph is showing.

Include the data from the legend (if available), the axes of the graph, and an interpretation of the data series.

eg. The following is an example description of a pie chart showing the distribution of sales for 3 product categories (A, B, C):

The pie chart shows the distribution of sales for 3 product categories (A, B, C), with each slice representing the percentage of total sales for each category.

Product Category A: 40%
Product Category B: 30%
Product Category C: 30%

"""

ITERATIVE_ANALYSIS_CHART_RULES_SCATTER = """Analyse the image of the scatter plot provided and precisely describe the relationship between the x and y axes in the graph.

Include the data from the legend (if available), the axes of the graph, and an interpretation of the data series.

eg. The following is an example description of a scatter plot showing the relationship between height and weight for a group of individuals:

The scatter plot shows the relationship between height (x-axis) and weight (y-axis) for a group of individuals.
The data points show a positive correlation between height and weight, with taller individuals generally having higher weights.
There was one outlier in the data, with an individual who was shorter than average but had a higher weight.

"""

ITERATIVE_ANALYSIS_CHART_RULES_HISTOGRAM = """Analyse the image of the histogram provided and precisely describe the distribution of the data in the graph.

Include the data from the legend (if available), the axes of the graph, and an interpretation of the data series.

eg. The following is an example description of a histogram showing the distribution of test scores for a group of students:

The histogram shows the distribution of test scores for a group of students, with the x-axis representing the test scores and the y-axis representing the frequency of each score.
The data is normally distributed, with the majority of students scoring between 70-80, and a few students scoring above 90.

Test Scores: 
 - 0-60: 0 students
 - 60-70: 10 students
 - 70-80: 25 students
 - 80-90: 15 students
 - 90-100: 5 students

"""

ITERATIVE_ANALYSIS_CHART_RULES_BOX = """Analyse the image of the box plot provided and precisely describe the distribution of the data in the graph.

Include the data from the legend (if available), the axes of the graph, and an interpretation of the data series.

eg. The following is an example description of a box plot showing the distribution of test scores for a group of students:

The box plot shows the distribution of test scores for a group of students, with the x-axis representing the test scores and the y-axis representing the frequency of each score.
The data is normally distributed, with the majority of students scoring between 70-80, and a few students scoring above 90.

Test Scores:
    - Median: 75
    - Q1: 70
    - Q3: 80
    - Min: 60
    - Max: 95

"""

ITERATIVE_ANALYSIS_CHART_RULES_TIME_SERIES = """Analyse the image of the time series plot provided and precisely describe the time period covered in the graph, along with any trends or patterns in the data.

Include the data from the legend (if available), the axes of the graph, and an interpretation of the data series.

eg. The following is an example description of a time series plot showing the stock price of a company over a 5-year period:

The time series plot shows the stock price of a company over a 5-year period, with the x-axis representing the time (years) and the y-axis representing the stock price in USD.
The data series shows the stock price for each year, with an overall upward trend in the stock price over the 5-year period.

Stock Price:
    - 2017: $50
    - 2018: $60
    - 2019: $70
    - 2020: $80
    - 2021: $90

"""

ITERATIVE_ANALYSIS_CHART_RULES_HEAT_MAP = """Analyse the image of the heat map provided and precisely describe the color scale and what the colors represent in the graph.

Include the data from the legend (if available), the axes of the graph, and an interpretation of the data series.

eg. The following is an example description of a heat map showing the temperature distribution across a region:

The heat map shows the temperature distribution across a region, with the x-axis representing the longitude and the y-axis representing the latitude.
The color scale ranges from blue (cold) to red (hot), with blue representing temperatures below 0°C and red representing temperatures above 30°C.

The region around latitude 40°N and longitude 100°W has the highest temperatures, with temperatures above 30°C.
The region around latitude 60°N and longitude 20°E has the lowest temperatures, with temperatures below 0°C.

There is a gradient of temperature from west to east, with temperatures increasing as you move eastward.
There are also areas of high temperature variability, with some regions experiencing rapid changes in temperature, likely due to significant variations in altitude of those areas (mountainous areas).
"""

ITERATIVE_ANALYSIS_CHART_RULES_NETWORK = """Analyse the image of the network graph provided and precisely describe the nodes and edges in the graph.

Include the data from the legend (if available), the axes of the graph, and an interpretation of the data series.

eg. The following is an example description of a network graph showing the connections between different cities:

The network graph shows the connections between different cities, with the nodes representing the cities and the edges representing the connections between them.
The data series shows the distance between each pair of cities, with the thickness of the edge representing the distance.

City A is connected to City B with a distance of 100 km, City C with a distance of 200 km, and City D with a distance of 150 km.
City B is connected to City C with a distance of 300 km, City D with a distance of 250 km, and City E with a distance of 200 km.
City C is connected to City D with a distance of 100 km, City E with a distance of 150 km, and City F with a distance of 250 km.
City D is connected to City E with a distance of 50 km, City F with a distance of 200 km, and City G with a distance of 300 km.
City E is connected to City F with a distance of 100 km, City G with a distance of 150 km, and City H with a distance of 200 km.
City F is connected to City G with a distance of 50 km, City H with a distance of 100 km, and City I with a distance of 150 km.
City G is connected to City H with a distance of 200 km, City I with a distance of 250 km, and City J with a distance of 300 km.

"""

ITERATIVE_ANALYSIS_CHART_RULES_VENN = """Analyse the image of the Venn diagram provided and precisely describe the sets and intersections in the graph.

Include the data from the legend (if available), the axes of the graph, and an interpretation of the data series.

eg. The following is an example description of a Venn diagram showing the overlap between three sets (A, B, C):

The Venn diagram shows the overlap between three sets (A, B, C), with the circles representing the sets and the intersections representing the overlap between the sets.
The data series shows the number of elements in each set and the number of elements in each intersection.

Set A: 100 elements
Set B: 150 elements
Set C: 200 elements
Intersection of A and B: 50 elements
Intersection of A and C: 75 elements
Intersection of B and C: 100 elements
Intersection of A, B, and C: 25 elements

"""

ITERATIVE_ANALYSIS_CHART_RULES_SANKEY = """Analyse the image of the Sankey diagram provided and precisely describe the flow of data in the graph.

Include the data from the legend (if available), the axes of the graph, and an interpretation of the data series.

eg. The following is an example description of a Sankey diagram showing the flow of energy in a system:

The Sankey diagram shows the flow of energy in a system, with the nodes representing the energy sources and the edges representing the flow of energy between the sources.
The data series shows the amount of energy flowing between each source and the total energy flow in the system.

Energy Source A: 100 units
Energy Source B: 150 units
Energy Source C: 200 units
Energy Source D: 50 units

Total Energy Flow: 500 units

"""

ITERATIVE_ANALYSIS_CHART_RULES_TREE = """Analyse the image of the tree diagram provided and precisely describe the hierarchy of the data in the graph.

Include the data from the legend (if available), the axes of the graph, and an interpretation of the data series.

eg. The following is an example description of a tree diagram showing the hierarchy of a company's organizational structure:

The tree diagram shows the hierarchy of a company's organizational structure, with the nodes representing the staff at different levels of the organization and the edges representing the relationships between the levels.

Here's the hierarchy of the company's organizational structure:

CEO
├── VP
│   ├── Director
│   │   ├── Manager
│   │   │   ├── Supervisor
│   │   │   │   └── Employee
│   │   │   │   └── Employee
│   │   │   │   └── Employee
│   │   │   └── Supervisor
│   │   │   |   └── Employee
│   │   │   │   └── Employee
│   │   │   │   └── Employee
│   │   │   │   └── Employee
│   │   └── Manager
│   │       ├── Supervisor
│   │       │   └── Employee
│   │       │   └── Employee
│   │       └── Supervisor
│   │           └── Employee
│   └── Director
│       ├── Manager
│       │   ├── Supervisor
│       │   │   └── Employee
│       │   │   └── Employee
│       │   └── Emplyee
|       │   └── Emplyee
│       └── Manager
│           ├── Supervisor
│           │   └── Employee
│           └── Supervisor
│               └── Employee
│               └── Employee
│               └── Employee
└── VP 
    ├── Director
    │   ├── Employee
    │   └── Manager
    │       ├── Supervisor
    │       │   └── Employee
    │       │   └── Employee
    │       │   └── Employee
    │       └── Employee
    └── Employee
"""

ITERATIVE_ANALYSIS_CHART_RULES_RADAR = """Analyse the image of the radar chart provided and precisely describe the axes and data series in the graph.

Include the data from the legend (if available), the axes of the graph, and an interpretation of the data series.

eg. The following is an example description of a radar chart showing the performance of a company in different areas:

The radar chart shows the performance of a company in different areas, with the axes representing the areas of performance and the data series representing the performance in each area.

The company's performance in the following areas is shown in the radar chart:

Area A: 80%
Area B: 70%
Area C: 60%
Area D: 50%

"""

ITERATIVE_ANALYSIS_CHART_RULES_BUBBLE = """Analyse the image of the bubble chart provided and precisely describe the size and color of the bubbles in the graph.

Include the data from the legend (if available), the axes of the graph, and an interpretation of the data series.

eg. The following is an example description of a bubble chart showing the population and GDP of different countries:

The bubble chart shows the population and GDP of different countries, with the x-axis representing the population, the y-axis representing the GDP, the size of the bubbles representing the population, and the color representing the continent.

The following is the data for the countries in the bubble chart:

Country A: Population: 100M, GDP: $1T
Country B: Population: 50M, GDP: $500B
Country C: Population: 200M, GDP: $2T

"""

ITERATIVE_ANALYSIS_CHART_RULES_WATERFALL = """Analyse the image of the waterfall chart provided and precisely describe the starting and ending values in the graph.

Include the data from the legend (if available), the axes of the graph, and an interpretation of the data series.

eg. The following is an example description of a waterfall chart showing the revenue and expenses of a company:

The waterfall chart shows the revenue and expenses of a company, with the x-axis representing the categories of revenue and expenses and the y-axis representing the amount in USD.

The following is the data for the waterfall chart:

Starting Value: $100M
Revenue: $50M
Expenses: $30M
Ending Value: $120M

"""

ITERATIVE_ANALYSIS_CHART_RULES_GANTT = """Analyse the image of the Gantt chart provided and precisely describe the tasks and time periods in the graph.

Include the data from the legend (if available), the axes of the graph, and an interpretation of the data series.

eg. The following is an example description of a Gantt chart showing the tasks and time periods for a project:

The Gantt chart shows the tasks and time periods for a project, with the x-axis representing the time period and the y-axis representing the tasks.

The following is the data for the Gantt chart:

Task A: Start: Jan 1, End: Jan 15, Status: COMPLETE
Task B: Start: Jan 5, End: Jan 20, Status: IN PROGRESS
Task C: Start: Jan 10, End: Jan 25, Status: PENDING

"""

ITERATIVE_ANALYSIS_CHART_RULES_OTHER = """Analyse the image of the chart provided and describe the content within the image as precisely as possible.

Include the data from the legend (if available), the axes of the graph, and an interpretation of the data series.

eg. The following is an example description of a chart showing the distribution of sales for 3 product categories (A, B, C):

The chart shows the distribution of sales for 3 product categories (A, B, C), with each slice representing the percentage of total sales for each category.

Product Category A: 40%
Product Category B: 30%
Product Category C: 30%

"""


ITERATIVE_ANALYSIS_FORMULA = """Analyse the image of the equation provided and return the equation in LaTeX format.

eg. If the image contains the equation that is the integral symbol (∫) followed by the lower limit of integration (a) and the upper limit of integration (b), where the integrand function (x^2) and the differential (dx) indicates that the variable of integration is (x).


Then, return the latex format of the equation as follows, eg: 

$$
\int_{a}^{b} x^2 dx
$$

"""

ITERATIVE_ANALYSIS_TEXT = """Analyse the image of the text provided and precisely extract the text from the image.

Return only the text of the image, without any additional information or commentary.
"""

ITERATIVE_ANALYSIS_PICTURE_DIAGRAM = """Analyse the image of the diagram provided and describe the diagram as precisely as possible.

If the diagram is a flowchart, describe how the process flows between tasks, incuding listing the tasks, their dependencies and the decision points in the process.

If the diagram is a schematic, describe the components of the system, their relationships and the flow of data between them.

If the diagram is a circuit diagram, describe the components of the circuit, their connections and the flow of current between them.

If the diagram is a map, describe the geographical features, landmarks and the routes between them.

If the diagram is a chart, describe the data series, axes and the interpretation of the data.

If the diagram is a graph, describe the nodes and edges, their relationships and the flow of data between them.

"""

ITERATIVE_ANALYSIS_PICTURE_PHOTO = """Analyse the image of the photo provided and describe the content of the photo as precisely as possible.

If the photo is a portrait, describe the person's appearance, clothing, expression and pose.

If the photo is a landscape, describe the geographical features, landmarks and the environment.

If the photo is a still life, describe the objects, their arrangement and the lighting.

If the photo is a group photo, describe the people, their relationships and the context of the photo.

If the photo is a candid shot, describe the moment captured, the emotions and the setting.

"""

ITERATIVE_ANALYSIS_PICTURE_DRAWING = """Analyse the image of the drawing provided and describe the content of the drawing as precisely as possible.

If the drawing is a sketch, describe what the sketch is of, the style of the sketch and the medium used.

If the drawing is a doodle, describe the random patterns, shapes and lines in the doodle.

If the drawing is a caricature, describe the exaggerated features, the likeness to the subject and the humour in the caricature.

If the drawing is a cartoon, describe the characters, the setting and the storyline in the cartoon.

If the drawing is a painting, describe the subject, the style of the painting and the colours used.

"""

ITERATIVE_ANALYSIS_PICTURE_OTHER = """Analyse the image provided and describe the content of the image as precisely as possible."""

ITERATIVE_ANALYSIS_RADIOGRAPH_XRAY = """Analyse the image of the X-ray provided and describe the content of the X-ray as precisely as possible.
Include the date of the X-ray, the type of X-ray (eg. bone, chest, abdomen, skull, joint), and the location of the X-ray (eg. left arm, right leg, thoracic spine), if that information is visible.

If the X-ray is of a bone, describe the bone structure, any fractures or abnormalities in the bone, including the location + severity. Also, provide the name of the bone if it can be easily identified.

If the X-ray is of a chest, describe the lung structure, any abnormalities in the lungs, including the location + severity. Also, provide the name of the lung if it can be easily identified.

If the X-ray is of an abdomen, describe the organ structure, any abnormalities in the organs, including the location + severity.

If the X-ray is of a skull, describe the skull structure, any abnormalities in the skull, including the location + severity.

If the X-ray is of a joint, describe the joint structure, any abnormalities in the joint, including the location + severity. Also, provide the name of the joint if it can be easily identified.

"""

ITERATIVE_ANALYSIS_RADIOGRAPH_MRI = """Analyse the image of the MRI provided and describe the content of the MRI as precisely as possible.
Include the date of the MRI, the type of MRI (eg. T1, T2, FLAIR, DWI, etc.), and the sequence of the MRI (eg. axial, sagittal, coronal), if that information is visible.

If the MRI is of a brain, describe the brain structure, any abnormalities in the brain, including the location + severity of the abnormality.

If the MRI is of a spine, describe the spine structure, any abnormalities in the spine, including the location + severity of the abnormality.

If the MRI is of a knee, describe the knee structure, any abnormalities in the knee, including the location + severity of the abnormality.

If the MRI is of a shoulder, describe the shoulder structure, any abnormalities in the shoulder, including the location + severity of the abnormality.

If the MRI is of a hip, describe the hip structure, any abnormalities in the hip, including the location + severity of the abnormality.

"""

ITERATIVE_ANALYSIS_RADIOGRAPH_CT = """Analyse the image of the CT scan provided and describe the content of the CT scan as precisely as possible.
Inclnude the date of the CT scan, the type of CT scan (eg. head, chest, abdomen, pelvis, spine), and the sequence of the CT scan (eg. axial, sagittal, coronal), if that information is visible.

If the CT scan is of a head, describe the head structure, any abnormalities in the head, including the location + severity of the abnormality.

If the CT scan is of a chest, describe the chest structure, any abnormalities in the chest, including the location + severity of the abnormality.

If the CT scan is of an abdomen, describe the abdomen structure, any abnormalities in the abdomen, including the location + severity of the abnormality.

If the CT scan is of a pelvis, describe the pelvis structure, any abnormalities in the pelvis, including the location + severity of the abnormality.

If the CT scan is of a spine, describe the spine structure, any abnormalities in the spine, including the location + severity of the abnormality.

"""

ITERATIVE_ANALYSIS_RADIOGRAPH_OTHER = """Analyse the image of the radiograph provided and describe the content of the radiograph as precisely as possible.
Include the date of the radiograph, the type of radiograph (eg. X-ray, MRI, CT, PET), and the location of the radiograph (eg. left arm, right leg, thoracic spine), if that information is visible.
"""


ITERATIVE_ANALYSIS_OTHER = """Analyse the image provided and describe the content of the image as precisely as possible."""



DEFAULT_ANALYSIS_MESSAGE = """Analyse the user's image and follow the instructions of the first matching rule below: 

Rule 1: If the image looks like a table: Return the content of the table in markdown format. Be very precise in determining the data in each cell, and include the table headers if they are present.

Rule 2: If the image looks like a graph: Describe what the graph is showing, include the data from the legend (if available), the axes of the graph, and an interpretation of the data series. If there is a description of the graph in the image, include that as well.
  - If the graph is a bar chart, line chart, pie chart, etc., include that information in the description.
  - If the graph is a scatter plot, include the relationship between the x and y axes in the description.
  - If the graph is a histogram, include the distribution of the data in the description.
  - If the graph is a box plot, include the distribution of the data in the description.
  - If the graph is a time series plot, include the time period covered in the description, along with any trends or patterns in the data.
  - If the graph is a heat map, include the color scale and what the colors represent in the description.
  - If the graph is a network graph, include the nodes and edges in the description.
  - If the graph is a Venn diagram, include the sets and intersections in the description.
  - If the graph is a Sankey diagram, include the flow of data in the description.  
  - If the graph is a tree diagram, include the hierarchy of the data in the description.
  - If the graph is a radar chart, include the axes and data series in the description.
  - If the graph is a bubble chart, include the size and color of the bubbles in the description.
  - If the graph is a waterfall chart, include the starting and ending values in the description.
  - If the graph is a Gantt chart, include the tasks and time periods in the description.


Rule 3: If the image looks like a formula or equation: Return the formula or equation in LaTeX format

Rule 4: If the image contains only text: Extract the text from the image and return only that text.

Rule 5: All other images, meticulously describe the content within the image as precisely as possible.

Do not describe what you are doing, and do not use wordage like "the image contains ...".

For your reference, the image is located in the following section of the document: {section_name}

Below, between [START PRIOR CONTEXT] and [END PRIOR CONTEXT], is a paragraph or two of text before the image (in markdown format):

[START PRIOR CONTEXT]
{prior_context}
[END PRIOR CONTEXT]

Below, between [START POST CONTEXT] and [END POST CONTEXT], is a paragraph or two of text after the image (in markdown format):

[START POST CONTEXT]
{post_context}
[END POST CONTEXT]

The information in the prior and post context may be helpful for determining both the context of the image and also the meaning of the content within.
"""
def analyse_chunk_image(chunk: DocumentChunk, llm: ChatOpenAI, analysis_msg:str = None) -> str:
    if not chunk.is_image():
        return None
    
    img_ext = chunk.metadata.get("ext", "png") if chunk.metadata else "png"
    return analyse_image_data(chunk.content, img_ext, llm, analysis_msg)


def analyse_image_data(data:bytes|str, img_ext:str, llm:ChatOpenAI, analysis_msg:str = None, max_retries:int = 3, section_name:str = None, prior_context:str = None, post_context:str = None) -> str:
    ## Base64 the image content (if it's bytes)

    base64_data = base64.b64encode(data).decode('utf-8') if type(data) is not str else data  # Assume already base64 if the image data is str
    msg = analysis_msg
    if analysis_msg is None:
        msg = DEFAULT_ANALYSIS_MESSAGE.format(
                    section_name=section_name if section_name is not None else "Unknown", 
                    prior_context=prior_context if prior_context is not None else "No prior context", 
                    post_context=post_context if post_context is not None else "No post context")

    messages = [
        {
            "role": "system",
            "type": "text",
            "content": msg
        },
        {
            "role": "user",
            "content": [{
                "type": "image_url",
                "image_url": {
                    "url": f'data:image/{img_ext};base64,{base64_data}',
                    "detail": "high"
                }
            }]
        }
    ]

    retries = max_retries
    for attempt in range(retries):
        try:
            return llm.generate(messages, streaming=False)
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(0.5 + (0.5 * attempt))
            else:
                raise e
            

def analyse_image_data_iteratively(data:bytes|str, img_ext:str, llm:ChatOpenAI, max_retries:int = 3, section_name:str = None, prior_context:str = None, post_context:str = None) -> str:
    analysis_msg = ITERATIVE_ANALYSIS_CLASSIFIER_STEP
    output = analyse_image_data(data, img_ext, llm, analysis_msg, max_retries, section_name, prior_context, post_context)
    if output is None:
        return output
    
    classifier_data = json.loads(output)
    category = classifier_data.get("category", None)
    sub_category = classifier_data.get("sub_category", None)
    if category is None:
        return None
    if sub_category is None:
        sub_category = "other"
    
    ## Select the appropriate prompt based on the category and sub-category
    prompt = None
    if category == "table": 
        if sub_category == "standard":
            prompt = ITERATIVE_ANALYSIS_TABLE_RULES_STANDARD
        elif sub_category == "matrix":
            prompt = ITERATIVE_ANALYSIS_TABLE_RULES_MATRIX
        elif sub_category == "pivot":
            prompt = ITERATIVE_ANALYSIS_TABLE_RULES_PIVOT
        elif sub_category == "cross-tab":
            prompt = ITERATIVE_ANALYSIS_TABLE_RULES_CROSSTAB
        elif sub_category == "nested":
            prompt = ITERATIVE_ANALYSIS_TABLE_RULES_NESTED
        else:
            prompt = ITERATIVE_ANALYSIS_TABLE_RULES_OTHER
    elif category == "chart":
        if sub_category == "bar":
            prompt = ITERATIVE_ANALYSIS_CHART_RULES_BAR
        elif sub_category == "line":
            prompt = ITERATIVE_ANALYSIS_CHART_RULES_LINE
        elif sub_category == "pie":
            prompt = ITERATIVE_ANALYSIS_CHART_RULES_PIE
        elif sub_category == "scatter":
            prompt = ITERATIVE_ANALYSIS_CHART_RULES_SCATTER
        elif sub_category == "histogram":
            prompt = ITERATIVE_ANALYSIS_CHART_RULES_HISTOGRAM
        elif sub_category == "box":
            prompt = ITERATIVE_ANALYSIS_CHART_RULES_BOX
        elif sub_category == "time-series":
            prompt = ITERATIVE_ANALYSIS_CHART_RULES_TIME_SERIES
        elif sub_category == "heat-map":
            prompt = ITERATIVE_ANALYSIS_CHART_RULES_HEAT_MAP
        elif sub_category == "network":
            prompt = ITERATIVE_ANALYSIS_CHART_RULES_NETWORK
        elif sub_category == "venn":
            prompt = ITERATIVE_ANALYSIS_CHART_RULES_VENN
        elif sub_category == "sankey":
            prompt = ITERATIVE_ANALYSIS_CHART_RULES_SANKEY
        elif sub_category == "tree":
            prompt = ITERATIVE_ANALYSIS_CHART_RULES_TREE
        elif sub_category == "radar":
            prompt = ITERATIVE_ANALYSIS_CHART_RULES_RADAR
        elif sub_category == "bubble":
            prompt = ITERATIVE_ANALYSIS_CHART_RULES_BUBBLE
        elif sub_category == "waterfall":
            prompt = ITERATIVE_ANALYSIS_CHART_RULES_WATERFALL
        elif sub_category == "gantt":
            prompt = ITERATIVE_ANALYSIS_CHART_RULES_GANTT
        else:
            prompt = ITERATIVE_ANALYSIS_CHART_RULES_OTHER
    elif category == "formula":
        prompt = ITERATIVE_ANALYSIS_FORMULA
    elif category == "text":
        prompt = ITERATIVE_ANALYSIS_TEXT
    elif category == "picture":
        if sub_category == "diagram":
            prompt = ITERATIVE_ANALYSIS_PICTURE_DIAGRAM
        elif sub_category == "photo":
            prompt = ITERATIVE_ANALYSIS_PICTURE_PHOTO
        elif sub_category == "drawing":
            prompt = ITERATIVE_ANALYSIS_PICTURE_DRAWING
        else:
            prompt = ITERATIVE_ANALYSIS_PICTURE_OTHER
    elif category == "radiograph":
        if sub_category == "x-ray":
            prompt = ITERATIVE_ANALYSIS_RADIOGRAPH_XRAY
        elif sub_category == "mri":
            prompt = ITERATIVE_ANALYSIS_RADIOGRAPH_MRI
        elif sub_category == "ct":
            prompt = ITERATIVE_ANALYSIS_RADIOGRAPH_CT
        else:
            prompt = ITERATIVE_ANALYSIS_RADIOGRAPH_OTHER
    else:
        prompt = ITERATIVE_ANALYSIS_OTHER
    
    output = analyse_image_data(data, img_ext, llm, prompt, max_retries, section_name, prior_context, post_context)
    return output



if __name__ ==  '__main__':
    from graphrag.query.llm.oai.chat_openai import ChatOpenAI
    from graphrag.query.llm.oai.typing import OpenaiApiType
    from pathlib import Path
    import os

    llm = None
    args = {}
    graphrag_config = None
    settings_path = Path("settings.yaml")
    if settings_path.exists():
        with settings_path.open("rb") as file:
            import yaml
            from graphrag.config import create_graphrag_config
            data = yaml.safe_load(file.read().decode(encoding="utf-8", errors="strict"))
            graphrag_config = create_graphrag_config(data, root_dir="./")

    grc_llm = graphrag_config.llm if graphrag_config is not None else None
    llm_model = next((v for v in [args.get('--openai-model'), os.environ.get('OPENAI_MODEL'), grc_llm.deployment_name if grc_llm is not None else None] if v is not None), 'gpt-4o')
    llm_api_key=next((v for v in [args.get('--openai-key'), os.environ.get('GRAPHRAG_API_KEY'), os.environ.get('OPENAI_API_KEY'), grc_llm.api_key if grc_llm is not None else None] if v is not None)) ## Will throw error if not of these options provide a key
    llm_api_base=next((v for v in [args.get('--openai-api-base'),os.environ.get('GRAPHRAG_API_BASE'), os.environ.get('OPENAI_API_BASE')] if v is not None)) ## Will throw error if not of these options provide a base url
    llm_api_version=next((v for v in [args.get('--openai-api-version'), os.environ.get('GRAPHRAG_API_VERSION'), os.environ.get('OPENAI_API_VERSION')] if v is not None), '2024-02-01')
    llm_api_retries=next((v for v in [args.get('--openai-api-retries'), os.environ.get('GRAPHRAG_API_RETRIES'), os.environ.get('OPENAI_API_RETRIES')] if v is not None), 3)
    llm_org_api=next((v for v in [args.get('--ad-org'), os.environ.get('GRAPHRAG_AD_ORG_ID'), os.environ.get('AD_ORG_ID')] if v is not None), None)
    llm = ChatOpenAI(
        api_key=llm_api_key,
        api_base=llm_api_base,
        organization=llm_org_api,
        model='gpt4o-mini',
        deployment_name=llm_model,
        api_type=OpenaiApiType.AzureOpenAI,
        api_version=llm_api_version,
        max_retries=llm_api_retries,
    )

    with open("images/2015_Increasing_stringiness_of_low_fat_mozzarella_string_cheese_using_polysaccharides,_Oberg_6_3_0.png", "rb") as f:
        data = f.read()
        print("Analysing Image...")
        try:
            output = analyse_image_data_iteratively(data, "png", llm)
        except Exception as e:
            print("Error: ", e)
            output = None

        print("Output: ", output)
