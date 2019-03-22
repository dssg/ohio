#!/usr/bin/env python
import matplotlib.pyplot as plt
import yaml


if __name__ == '__main__':
    with open('./profile/profiling-latest.yaml') as fd:
        data = yaml.load(fd)

    plt.scatter([v['memory'] for v in data.values()], [v['time'] for v in data.values()])
    for (key, value) in data.items():
        plt.annotate(key, (value['memory'], value['time']))

    plt.xlabel('memory (mb)')
    plt.ylabel('time (s)')

    plt.savefig('plot.png')
