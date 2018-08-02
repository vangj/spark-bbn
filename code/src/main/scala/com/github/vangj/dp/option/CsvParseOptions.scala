package com.github.vangj.dp.option

/**
  * Parse options for CSV.
  * @param hasHeaders Does the CSV have headers?
  * @param delimiter The delimiter character.
  * @param quote The quote character.
  * @param escape The escape character.
  */
case class CsvParseOptions(hasHeaders: Boolean, delimiter: Char, quote: Char, escape: Char)
