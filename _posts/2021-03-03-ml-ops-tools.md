---
toc: true
layout: post
comments: true
description: Overview of a few popular ML Workflow tools
categories: [ml, tools]
hide: true
title: "ML Engineering Workflow Tools"
---


# ML Engineering Workflow Tools

Dozens of ML Workflow tools have been created in the past few years amidst a growing realization that making ML models work in the wild is hard. In this post I overview three of the more popular open-source tools: DVC (Data Version Control), MLFlow, and Kedro. 

## Why is Data Science code so messy?

People often blame Jupyter Notebooks for permitting (and even encouraging?) Data Science messiness but I don't think notebooks are the main culprit here. A few reasons that come to mind:

* Data Scientists often come from academic research background (i.e. *the Physics PhD*), as opposed to software development disciplines so many are not experienced in building and maintaining large software projects. It is hard to find good resources on ML Engineering best practices so new Data Scientists have to learn these on the job.
* There is a mismatch between the engineering workflow (construction) and the data science / research workflow (deconstruction). The engineering workflow centers on building systems that handle complex processes in a reliable and maintainable manner. The research (data science) workflow uses observations from a complex process in attempt to extract the essential characteristics of this processes. While engineers stack together neat Lego bricks, data scientists try to untangle a messy ball of yarn.

## Quick Overview of the Tools

* DVC (Data Version Control) is used in conjunction with Git to version-control datasets. The actual data is typically stored in cloud storage such as S3 and DVC links the stored files and the project. Users interact with dvc via a command line utility (`add`, `push`, `pull`). 
* MLFlow 
* Kedro

## DVC

DVC is conceptually the simplest tool of the three. Datasets change frequently so they need to be versioned and tracked similar to code, if we want reproducible analysis. Version control tools such as Git do not handle well large files so DVC addresses this limitation. The interface is through the command line, for example:

```
dvc add my_dataset.parquet
```

will start tracking the dataset `my_dataset.parquet`. This command does two important things. First, it creates a small text file `my_dataset.parquet.dvc` with the following contents:

```
outs:
- md5: d63513d9b81cb819ac3466b375ef31d
  size: 2302490
  path: my_dataset.parquet
```

This includes the md5 hash of the dataset, its size and the path to the file. The `dvc add` command also copies the dataset to a local cache inside a `.dvc` folder: `./dvc/cache/d6/3513d9b81cb819ac3466b375ef31d` where the folder path is based on the md5 hash above. 

The small text file can be committed to git:

```
git add my_dataset.parquet.dvc .gitignore
git commit -m "Add my dataset"
```

while the actual cached dataset can be pushed to a cloud storage of our choice using:

```
dvc push
```

and conversely `dvc pull` to download it from the remote. You can find details on how to configure dvc with your cloud storage in their [tutorial](https://dvc.org/doc/start/data-versioning).


Personally, I tend to generate a lot of datasets (and dataset versions) when doing analysis and research so I find DVC very helpful. It is unobtrusive via its command-line interface - there is no need to change any of the existing code.

## MLFlow

## Kedro

## Conclusion

## References


