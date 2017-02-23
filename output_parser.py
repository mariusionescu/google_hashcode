import argparse
from collections import defaultdict


def parse(filename):
    CACHES = defaultdict(list)
    with open(filename) as f:
        for line in f:
            if len(line.split()) == 4:
                _, video_id, cache_id, _ = line.split()
                video_id = int(video_id)
                cache_id = int(cache_id)
                CACHES[cache_id].append(video_id)

    with open(filename.replace('.in', '.out'), 'w') as f:
        f.write('{}\n'.format(len(CACHES))) 
        for cache_id, videos in CACHES.iteritems():
            f.write('{} {}\n'.format(cache_id, ' '.join(map(str, videos))))


def main():
    parser = argparse.ArgumentParser(description="Google hashcode")

    parser.add_argument('-i', '--input', action='store')

    args = parser.parse_args()

    parse(args.input)

if __name__ == '__main__':
    main()
