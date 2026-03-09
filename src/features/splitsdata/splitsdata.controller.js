import ExcelJS from "exceljs";
import archiver from "archiver";
import fs from "fs";
import path from "path";
import { PassThrough } from "stream";
import { parse } from "csv-parse";
import { stringify } from "csv-stringify";

export const splitsData = async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ message: "File tidak ditemukan" });
    }

    const rowsPerFile = 1000;
    const filePath = req.file.path;
    const originalName = path.parse(req.file.originalname).name;
    const fileExtension = path.extname(req.file.originalname).toLowerCase();

    // Cleanup function
    const cleanup = () => {
      if (fs.existsSync(filePath)) {
        fs.unlink(filePath, (err) => {
          if (err) console.error("Gagal hapus:", err);
        });
      }
    };

    res.on("close", cleanup);
    res.on("finish", cleanup);

    res.setHeader("Content-Type", "application/zip");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename=${originalName}_split.zip`,
    );

    const archive = archiver("zip", { zlib: { level: 1 } });
    archive.pipe(res);

    let headers = [];
    let rowCount = 0;
    let fileIndex = 1;

    // Helper untuk membuat stream baru di dalam ZIP
    let currentPassThrough;
    let csvStringifier; // Khusus CSV
    let workbookWriter; // Khusus XLSX
    let worksheetWriter;

    const finalizeCurrentFile = async () => {
      if (fileExtension === ".csv") {
        if (csvStringifier) csvStringifier.end();
      } else {
        if (workbookWriter) await workbookWriter.commit();
      }
    };

    const createNewFileInZip = () => {
      currentPassThrough = new PassThrough();
      const newFileName = `${originalName}_part${fileIndex}${fileExtension}`;

      archive.append(currentPassThrough, { name: newFileName });

      if (fileExtension === ".csv") {
        csvStringifier = stringify({ header: true, columns: headers });
        csvStringifier.pipe(currentPassThrough);
      } else {
        workbookWriter = new ExcelJS.stream.xlsx.WorkbookWriter({
          stream: currentPassThrough,
        });
        worksheetWriter = workbookWriter.addWorksheet("Sheet1");
        worksheetWriter.addRow(headers).commit();
      }
      fileIndex++;
    };

    // LOGIKA PEMBACAAN (STREAMING)
    if (fileExtension === ".csv") {
      const parser = fs
        .createReadStream(filePath)
        .pipe(parse({ columns: false }));

      for await (const row of parser) {
        if (rowCount === 0) {
          headers = row;
          rowCount++; // Tandai header sudah diambil
          continue;
        }

        if ((rowCount - 1) % rowsPerFile === 0) {
          await finalizeCurrentFile();
          createNewFileInZip();
        }

        if (csvStringifier) csvStringifier.write(row);
        rowCount++;
      }
    } else {
      const workbookReader = new ExcelJS.stream.xlsx.WorkbookReader(filePath);
      for await (const worksheet of workbookReader) {
        for await (const row of worksheet) {
          if (row.number === 1) {
            headers = row.values.slice(1);
            continue;
          }

          if (rowCount % rowsPerFile === 0) {
            await finalizeCurrentFile();
            createNewFileInZip();
          }

          worksheetWriter.addRow(row.values.slice(1)).commit();
          rowCount++;
        }
      }
    }

    await finalizeCurrentFile();
    await archive.finalize();
  } catch (error) {
    console.error(error);
    if (!res.headersSent) res.status(500).json({ error: error.message });
  }
};
