<?php

namespace App;

final class Parser
{
    private const int OFFSET_SITE = 25;

    public function parse(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'r');
        $data = [];

        while ($line = fgets($handle)) {
            $offset = strcspn($line, ',');
            $path = '/blog/' . substr($line, offset: self::OFFSET_SITE, length: $offset - self::OFFSET_SITE);
            $date = substr($line, offset: $offset + 1, length: 10);
            $data[$path][$date] ??= 0;
            $data[$path][$date] += 1;
        }

        foreach ($data as &$visits) {
            ksort($visits);
        }

        file_put_contents($outputPath, json_encode($data, JSON_PRETTY_PRINT));

        fclose($handle);
    }
}