from sys import argv
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
from itertools import cycle

sns.set_theme("paper")
sns.set_style("whitegrid")
hatches = ["////", "\\\\\\\\", "...", "||||", "xx"]
colormap = sns.light_palette("gray", as_cmap=True, reverse=True)

dfr = pd.read_csv("data.csv")

dfk = dfr.loc[dfr["dag_id"] == "verified_kmeans_pipeline"]

scenarios = ['CC', 'PC', 'EC', 'IC']

dfm = dfk[["Probe type"]].fillna("Pipeline") \
    .join(dfk[scenarios].astype(float).mul(dfk["Execution time (s)"], axis=0))\
    .groupby("Probe type").sum()\
    .transpose()[["Pipeline", "General purpose probe", "Pipeline probe", "Service probe", "Task probe"]]


ax = dfm.plot.bar(stacked=True, colormap=colormap, edgecolor="k",
                  rot=0, ylabel="Execution time (s)", xlabel="Scenario")

# Set hatches
for container, hatch in zip(ax.containers, cycle(hatches)):
    for patch in container.patches:
        patch.set_hatch(hatch)

# Set legend
ax.legend(bbox_to_anchor=(0.5, 1.0), loc="lower center", ncol=3)

if len(argv) > 1:
    for arg in argv[1:]:
        plt.savefig(arg)
else:
    plt.savefig("fig1.png")
