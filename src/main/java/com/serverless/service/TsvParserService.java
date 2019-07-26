package com.serverless.service;

import com.univocity.parsers.common.DataProcessingException;
import com.univocity.parsers.conversions.FormattedConversion;
import com.univocity.parsers.conversions.ObjectConversion;
import com.univocity.parsers.tsv.TsvParserSettings;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class TsvParserService {

  public static List<String[]> parseCards(InputStream inputStream) {
    TsvParserSettings settings = new TsvParserSettings();
    settings.getFormat().setLineSeparator("\n");

    com.univocity.parsers.tsv.TsvParser parser = new com.univocity.parsers.tsv.TsvParser(settings);

    return parser.parseAll(inputStream);
  }

  public static class DateTimeConversion
      extends ObjectConversion<LocalDateTime>
      implements FormattedConversion<SimpleDateFormat> {

    private final Locale locale;
    private final ZoneId timeZoneId;
    private final DateTimeFormatter[] parsers;
    private final String[] formats;

    DateTimeConversion(String... dateFormats) {
      this(null, null, dateFormats);
    }

    DateTimeConversion(ZoneId timeZoneId, Locale locale, String... dateFormats) {
      this.timeZoneId = timeZoneId == null ? ZoneOffset.UTC.normalized() : timeZoneId;
      this.locale = locale == null ? Locale.US : locale;
      this.formats = dateFormats.clone();
      this.parsers = new DateTimeFormatter[dateFormats.length];
      for (int i = 0; i < dateFormats.length; i++) {
        String dateFormat = dateFormats[i];
        parsers[i] = DateTimeFormatter.ofPattern(dateFormat, this.locale).withZone(this.timeZoneId);
      }
    }

    @Override
    public String revert(LocalDateTime input) {
      if (input == null) {
        return super.revert(null);
      }
      return parsers[0].format(input);
    }

    @Override
    public SimpleDateFormat[] getFormatterObjects() {
      return new SimpleDateFormat[0];
    }

    @Override
    protected LocalDateTime fromString(String input) {
      for (DateTimeFormatter formatter : parsers) {
        try {
          return LocalDateTime.parse(input, formatter);
        } catch (DateTimeParseException ex) {
          //ignore and continue
        }
      }
      DataProcessingException exception = new DataProcessingException(
          "Cannot parse '{value}' as a valid date of locale '" + locale + "'. Supported formats are: " + Arrays
              .toString(formats));
      exception.setValue(input);
      throw exception;
    }
  }
}
