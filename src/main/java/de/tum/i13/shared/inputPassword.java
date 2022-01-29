package de.tum.i13.shared;

public class inputPassword {
    private int countPasswordInput = 0;
    private boolean inputPassword = false;

    public synchronized int getCountPasswordInput() {
        return countPasswordInput;
    }

    public synchronized void setCountPasswordInput(int countPasswordInput) {
        this.countPasswordInput = countPasswordInput;
    }

    public synchronized boolean isInputPassword() {
        return inputPassword;
    }

    public synchronized void setInputPassword(boolean inputPassword) {
        this.inputPassword = inputPassword;
    }

    public inputPassword(boolean inputPassword, int countPasswordInput) {
        this.inputPassword = inputPassword;
        this.countPasswordInput = countPasswordInput;
    }

    public synchronized void increaseCounter() {
        this.countPasswordInput++;
    }

}
