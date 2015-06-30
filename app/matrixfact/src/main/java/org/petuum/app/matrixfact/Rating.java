package org.petuum.app.matrixfact;

// A rating from a user on a product.
public class Rating {
    public final int userId;
    public final int prodId;
    public final float rating;

    public Rating(int userId, int prodId, float rating) {
        this.userId = userId;
        this.prodId = prodId;
        this.rating = rating;
    }
}
