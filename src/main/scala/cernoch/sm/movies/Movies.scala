package cernoch.sm.movies

import cernoch.scalogic._
import cernoch.scalogic.storage.Dumpable
import java.io.StringReader
import java.lang.String
import java.text.Normalizer
import org.supercsv.io.CsvListReader
import org.supercsv.prefs.CsvPreference
import scala.collection.Iterable
import scala.collection.JavaConverters._



object Movies extends Dumpable[BLC[Atom[Val[_]]]] {

  /**
   * Returns all atoms in the dataset
   */
  def dump
  : Iterable[BLC[Atom[Val[_]]]]
  = for (list <- List(Data.people, Data.film, Data.casts, Data.actors);
         atom <- list )
    yield BLC(atom)

  /**
   * All domains used by the data
   */
  object domains {
    val year  = NumDom("year")
    val years = NumDom("years")
    val sex   = CatDom("sex",false,Set("M","F"))

    val genre       = CatDom("genre")
    val nationality = CatDom("nationality")

    val actor  = CatDom("actor",true)
    val filmid = CatDom("filmid",true)
    val person = CatDom("person",true)
    val studio = CatDom("studio",true)

    def apply() = Set(
      actor, year, years,
      sex, genre, nationality,
      filmid, person, studio
    )
  }

  /**
   * Schema of the dataset
   */
  object schema {
    val actor    = Btom("actor{func,fneq=3:4}(+actor, -year, -years, -years, -sex, -nationality)", domains())
    val person   = Btom("person{func,fneq=3:4}(+person, -year, -years, -years)", domains())
    val cast     = Btom("cast(+filmid, -actor)", domains())

    val film     = Btom("film(-filmid, -year, -person, -studio)", domains())
    val genre    = Btom("genre(+filmid, -genre)", domains())
    val producer = Btom("producer(+filmid, -person)", domains())

    def apply() = Set(
      actor, person, cast,
      film, genre, producer
    )
  }

  /**
   * Lists all data in this dataset
   */
  object Data {

    /**
     * Removes anything before ":" in the string
     */
    private def remPrefix
    (s:String)
    = s.replaceAll(".*: *", "")

    /**
     * If string is empty, return null, otherwise the string itself
     */
    private def zeroNull
    (s:String)
    = if (s.length() == 0) null else s


    /**
     * Removes accents+spaces and normalizes the string
     */
    private def simplify
      (s:String)
    = Normalizer.normalize(s, Normalizer.Form.NFD)
      .replaceAll("\\p{InCombiningDiacriticalMarks}+", "")
      .toLowerCase.replaceAll("[^a-z0-9]+", "_")

    def people = lists("people.csv").map{ line =>

      val name = line(0)
      val act  = line(3).replaceFirst("^@","")
      val born = line(6)

      val yearBorn = Year.unapply(born)

      val (yearStarted, activeYears) = act match {
        case YearRange(beg,end) => (Some(beg), Some(end-beg))
        case Year(year) => (Some(year), None)
        case _ => (None, None)
      }

      val ageStarted = (yearBorn, yearStarted) match {
        case (Some(b), Some(s)) => Some(s - b)
        case _ => None
      }

      val args =
        new Cat(simplify(name),     domains.person) ::
        new Num(yearStarted.orNull, domains.year)  ::
        new Num(activeYears.orNull, domains.years) ::
        new Num(ageStarted.orNull,  domains.years) ::
        Nil

      schema.person.mapAllArgs((schema.person.args zip args).toMap)
    }

    def film = filmTriples.map{
      case (f,g,h) => List(f) ++ g ++ h
    }.flatten

    def filmTriples = lists("main.csv").map{ line =>

      val filmid   = line(0)
      val produced = line(2)
      val director = remPrefix(line(3))
      val producer = remPrefix(line(4)).split(",")
      val studio   = remPrefix(line(5))
      val genres   = (Option(line(7)) getOrElse "").split(",")

      val year = Year.unapply(produced)

      val args =
        new Cat(filmid,             domains.filmid)  ::
        new Num(year.orNull,        domains.year)    ::
        new Cat(simplify(director), domains.person)  ::
        new Cat(simplify(studio),   domains.studio)  ::
        Nil

      val film = schema.film.mapAllArgs((schema.film.args zip args).toMap)

      val genre = genres
        .map( simplify )
        .map( zeroNull )
        .filter{ _ != null }
        .map{ genre =>
          val args =
            new Cat(filmid, domains.filmid) ::
            new Cat(genre,  domains.genre)  ::
            Nil

          schema.genre.mapAllArgs((schema.genre.args zip args).toMap)
      }

      val prod = producer
        .map( simplify )
        .map( zeroNull )
        .filter{ _ != null }
        .map{ producer =>
          val args =
            new Cat(filmid,   domains.filmid) ::
            new Cat(producer, domains.person) ::
            Nil
          schema.producer.mapAllArgs((schema.producer.args zip args).toMap)
        }


      (film, genre.toList, prod.toList)
    }

    def casts = lists("casts.csv").map { line =>

      val film = line(0)
      val name = line(2)

      val args =
        new Cat(film,           domains.filmid)  ::
        new Cat(simplify(name), domains.actor)  ::
        Nil

      schema.cast.mapAllArgs((schema.cast.args zip args).toMap)
    }

    def actors = lists("actors.csv").map{ line =>

      val name = line(0)
      val act  = line(1)
      val sex  = line(4)
      val born = line(5)
      val nat  = line(8)

      val yearBorn = Year.unapply(born)

      val (yearStarted, activeYears) = act match {
        case YearRange(beg,end) => (Some(beg), Some(end-beg))
        case Year(year) => (Some(year), None)
        case _ => (None, None)
      }

      val ageStarted = (yearBorn, yearStarted) match {
        case (Some(b), Some(s)) => Some(s - b)
        case _ => None
      }

      val args =
      new Cat(simplify(name),     domains.actor) ::
      new Num(yearStarted.orNull, domains.year)  ::
      new Num(activeYears.orNull, domains.years) ::
      new Num(ageStarted.orNull,  domains.years) ::
      new Cat(sex,                domains.sex)   ::
      new Cat(nat,                domains.nationality) ::
        Nil

      schema.actor.mapAllArgs((schema.actor.args zip args).toMap)
    }
  }

  object Year {
    val REG = "([0-9]{4})".r

    def unapply(s:String) = s match {
      case REG(year) => Some(BigInt(year.toInt))
      case _ => None
    }
  }

  object YearRange {
    val REG = "([0-9]{4})-([0-9]{4})".r

    def unapply(s:String) = s match {
      case REG(from, to) =>
        Some((BigInt(from.toInt), BigInt(to.toInt)))
      case _ => None
    }
  }

  /**
   * Load the CSV file from the resources and parse that...
   */
  def lists(name:String)
  = lines(name)
    .map{ new StringReader(_) }
    .map{ new CsvListReader(_, CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE) }
    .map{ _.read().asScala.map{Option(_)}.map{_ getOrElse ""} }

  private def lines(name:String) = source(name) match {
    case None => throw new Exception("File not found.")
    case Some(v) => v.getLines()
  }

  private def source(name:String) = stream(name) match {
    case Some(v) => Some(io.Source.fromInputStream(v))
    case None => None
  }

  private def stream(name: String) = {
    val fname = "cernoch/sm/movies/data/" + name
    val stream = getClass.getClassLoader.getResourceAsStream(fname)
    if (stream == null) None else Some(stream)
  }

  /**
   * Prints the content of the dataset
   */
  def main(args: Array[String])
  = dump.map(println)
}
