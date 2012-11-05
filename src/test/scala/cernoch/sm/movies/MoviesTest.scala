package cernoch.sm.movies

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MoviesTest extends Specification {

  "Movie dataset" should {

    "read all records" in {
      Movies.dump.size must_== 23 * 3709
    }
  }
}