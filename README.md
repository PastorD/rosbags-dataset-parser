# Parse Rosbags

Load data from a rosbag file, use it in python or export it to a different format. 

How to use it? 
- As a library in Python
```python
import rosbags-parser as rp
df = rp.load_data("input_rosbag_folder", {"topic",: ["field1","field2"]})
...(use dataframe df)
```
- As a terminal tool
```bash
rosbags-parser <input_rosbag_folder> <output_data.file_extension>
```