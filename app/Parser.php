<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $handle = fopen($inputPath, 'r');
        $data = [];

        while ($line = fgetcsv($handle, escape: ',')) {
            $path = str_replace('https://stitcher.io', '', $line[0]);
            $year = substr($line[1], offset: 0, length: 10);
            $data[$path][$year] ??= 0;
            $data[$path][$year] += 1;
        }

        foreach ($data as &$visits) {
            ksort($visits);
        }

        file_put_contents($outputPath, json_encode($data, JSON_PRETTY_PRINT));

        fclose($handle);
    }
}