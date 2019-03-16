import com.jakewharton.retrofit2.adapter.kotlin.coroutines.CoroutineCallAdapterFactory
import kotlinx.coroutines.Deferred
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory
import retrofit2.http.GET
import retrofit2.http.Path
import retrofit2.http.Url

fun createService(): Service {
    return Retrofit.Builder()
        .baseUrl("https://swapi.co/api/")
        .addCallAdapterFactory(CoroutineCallAdapterFactory())
        .addConverterFactory(GsonConverterFactory.create())
        .build()
        .create(Service::class.java)
}

interface Service {
    @GET("planets/{id}/")
    fun getPlanet(@Path("id") id: Int): Deferred<Planet>

    @GET
    fun getFilmFullUrl(@Url url: String): Deferred<Film>
}

data class Planet(val name: String, val films: List<String>)

data class Film(val title: String, val episode_id: Int, val opening_crawl: String)