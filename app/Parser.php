<?php

namespace App;

final class Parser
{
    private const int OFFSET_SITE = 25;

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);
        $numWorkers = min(8, max(1, (int) ceil($fileSize / 262144)));

        if ($numWorkers <= 1) {
            $result = $this->processChunk($inputPath, 0, $fileSize);
            $pathOrder = array_fill_keys($result['paths'], true);
            $this->writeJson($outputPath, $result['data'], $pathOrder);
            return;
        }

        $useIgbinary = function_exists('igbinary_serialize');
        $chunkSize = (int) ceil($fileSize / $numWorkers);
        $tmpPrefix = sys_get_temp_dir() . '/100m_' . getmypid() . '_';
        $pids = [];

        for ($w = 0; $w < $numWorkers - 1; $w++) {
            $start = $w * $chunkSize;
            $end = ($w + 1) * $chunkSize;

            $pid = pcntl_fork();

            if ($pid === 0) {
                $result = $this->processChunk($inputPath, $start, $end);
                file_put_contents(
                    $tmpPrefix . $w,
                    $useIgbinary ? igbinary_serialize($result) : serialize($result),
                );
                exit(0);
            }

            $pids[] = $pid;
        }

        $parentResult = $this->processChunk($inputPath, ($numWorkers - 1) * $chunkSize, $fileSize);

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $data = [];
        $pathOrder = [];

        for ($w = 0; $w < $numWorkers - 1; $w++) {
            $file = $tmpPrefix . $w;
            $raw = file_get_contents($file);
            unlink($file);
            $result = $useIgbinary ? igbinary_unserialize($raw) : unserialize($raw);

            foreach ($result['paths'] as $path) {
                $pathOrder[$path] ??= true;
            }

            foreach ($result['data'] as $path => $dates) {
                if (!isset($data[$path])) {
                    $data[$path] = $dates;
                } else {
                    foreach ($dates as $date => $count) {
                        $data[$path][$date] = ($data[$path][$date] ?? 0) + $count;
                    }
                }
            }
        }

        foreach ($parentResult['paths'] as $path) {
            $pathOrder[$path] ??= true;
        }
        foreach ($parentResult['data'] as $path => $dates) {
            if (!isset($data[$path])) {
                $data[$path] = $dates;
            } else {
                foreach ($dates as $date => $count) {
                    $data[$path][$date] = ($data[$path][$date] ?? 0) + $count;
                }
            }
        }

        $this->writeJson($outputPath, $data, $pathOrder);
    }

    private function processChunk(string $inputPath, int $startOffset, int $endOffset): array
    {
        $fp = fopen($inputPath, 'r');

        if ($startOffset > 0) {
            fseek($fp, $startOffset - 1);
            if (fread($fp, 1) !== "\n") {
                fgets($fp);
            }
        }

        $data = [];
        $orderedPaths = [];
        $remaining = '';
        $lastPath = '';
        /** @noinspection PhpUnusedLocalVariableInspection */
        $pathRef = null;
        $os = self::OFFSET_SITE;

        while (($fpPos = ftell($fp)) < $endOffset) {
            $chunk = fread($fp, min(4194304, $endOffset - $fpPos));
            if ($chunk === false || $chunk === '') break;

            if ($remaining !== '') {
                $chunk = $remaining . $chunk;
                $remaining = '';
            }

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) {
                $remaining = $chunk;
                continue;
            }

            if ($lastNl < strlen($chunk) - 1) {
                $remaining = substr($chunk, $lastNl + 1);
            }

            $pos = 0;
            while ($pos < $lastNl) {
                $commaPos = strpos($chunk, ',', $pos + $os);
                if ($commaPos === false || $commaPos + 26 > $lastNl) break;

                $path = substr($chunk, $pos + $os, $commaPos - $pos - $os);
                $date = substr($chunk, $commaPos + 1, 10);

                if ($path !== $lastPath) {
                    $lastPath = $path;
                    if (!isset($data[$path])) {
                        $orderedPaths[] = $path;
                        $data[$path] = [];
                    }
                    $pathRef = &$data[$path];
                }

                $pathRef[$date] = ($pathRef[$date] ?? 0) + 1;

                $pos = $commaPos + 27;
            }
        }

        if ($remaining !== '') {
            $rest = fgets($fp);
            if ($rest !== false) {
                $remaining .= $rest;
            }
            $remaining = rtrim($remaining, "\n\r");
            if (strlen($remaining) > $os) {
                $commaPos = strpos($remaining, ',', $os);
                if ($commaPos !== false) {
                    $path = substr($remaining, $os, $commaPos - $os);
                    $date = substr($remaining, $commaPos + 1, 10);

                    if ($path !== $lastPath) {
                        if (!isset($data[$path])) {
                            $orderedPaths[] = $path;
                            $data[$path] = [];
                        }
                        $pathRef = &$data[$path];
                    }

                    $pathRef[$date] = ($pathRef[$date] ?? 0) + 1;
                }
            }
        }

        fclose($fp);

        return ['data' => $data, 'paths' => $orderedPaths];
    }

    private function writeJson(string $outputPath, array &$data, array &$pathOrder): void
    {
        $out = fopen($outputPath, 'w');
        stream_set_write_buffer($out, 1 << 20);
        fwrite($out, "{\n");

        $first = true;
        foreach ($pathOrder as $path => $_) {
            $visits = &$data[$path];
            ksort($visits);

            $block = '';
            if (!$first) $block = ",\n";
            $first = false;

            $block .= '    "\/blog\/' . $path . '": {' . "\n";

            $firstDate = true;
            foreach ($visits as $date => $count) {
                if (!$firstDate) $block .= ",\n";
                $firstDate = false;
                $block .= '        "' . $date . '": ' . $count;
            }

            $block .= "\n    }";
            fwrite($out, $block);
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
